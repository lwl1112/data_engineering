import csv
import os
import datetime
import collections
import sys

TIME_FORMAT="%Y-%m-%d %H:%M:%S"
sessiondict=collections.defaultdict(list) #{end_session: [ip]}
session=dict() #{ip: start, end, duration, requests}

if __name__ == "__main__":
	if(len(sys.argv)!=4):
		raise SystemExit("python ./src/sessionization.py ./input/log.csv ./input/inactivity_period.txt ./output/sessionization.txt\n")
	else:
		logfile = os.path.join(os.curdir, sys.argv[1])
		inactivityfile = os.path.join(os.curdir, sys.argv[2])
		outfile = os.path.join(os.curdir, sys.argv[3])
		
# create a session for each ip
def startSession(log_row, inactivity_period):
	ip = log_row['ip']
	curr_time = datetime.datetime.strptime(log_row['date']+" "+log_row['time'], TIME_FORMAT)
	end_time = curr_time + datetime.timedelta(seconds=inactivity_period+1)
	sessiondict[end_time].append(ip)
	
	return ip, curr_time, sessiondict

# remove session after inactivity period
def endSession(curr_ip, curr_time, sessiondict, inactivity_period):
	if curr_time in sessiondict.keys():
		for ip in sessiondict[curr_time]:
			if session[ip]['end'] == curr_time-datetime.timedelta(seconds=inactivity_period+1):
				printOut(session,ip)
				sessiondict[curr_time].remove(ip)
				del session[ip]
	
	if curr_ip not in session:
		session[curr_ip] = {'start':curr_time,'end':curr_time,'duration':1,'requests':1}
	else:
		session[curr_ip]['requests'] += 1
		session[curr_ip]['end'] = curr_time
		session[curr_ip]['duration'] = int((session[curr_ip]['end'] - session[curr_ip]['start']).total_seconds())+1
		
	return sessiondict,session

# write	to file
def printOut(session,ip):
    with open(outfile,'a') as the_file:
		writer = csv.writer(the_file)
		output = [ip,session[ip]['start'].strftime(TIME_FORMAT),session[ip]['end'].strftime(TIME_FORMAT),session[ip]['duration'],session[ip]['requests']]
		writer.writerow(output)

# update in the end with all expiring sessions
def updateSession(sessiondict,session):
	update = []
	for key,values in sessiondict.items():
		for value in values:
			if value not in update:
				printOut(session,value)
				update.append(value)

				
try:	
	f = open(outfile,'w')
	
	with open(inactivityfile) as a:
		inactivity = a.readlines()
		inactivity_period = int(inactivity[0])

	with open(logfile) as b:
		logs = csv.DictReader(b)
		for log_row in logs:
			curr_ip, curr_time, sessiondict = startSession(log_row, inactivity_period)
			sessiondict, session = endSession(curr_ip, curr_time, sessiondict, inactivity_period)
			
	updateSession(sessiondict, session)
	f.close()
except IOError as e:
	print('Failed: %s') % e.strerror
