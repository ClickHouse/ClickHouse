#!/bin/sh
if [[ $# -ne 0 ]]; then
    echo "usage: if memory limit is exceeded kill process with biggest memory consumption"
    exit 1
fi

while [ 1=1 ];
do
    FREE_MEMORY_MB=$(free -m | sed -n '2,2p' | awk '{print $4}')

    PID="$(ps -eF --sort -rss | sed -n '2,2p' | awk '{print $2}')"
    NAME="$(ps -eF --sort -rss | sed -n '2,2p' | awk '{print $11}')"
    SIZEGB="$(ps -eF --sort -rss | sed -n '2,2p' | awk '{print $6}')"
    SIZEGB=$(($SIZEGB/1024/1024))

    echo "Process id ="$PID" Size = "$SIZEGB" GB" "Free Memory = " $FREE_MEMORY_MB" MB"
    if (( $FREE_MEMORY_MB < 512 ));
    then echo "Killing the process with biggest memory consumption......"
	sudo kill -9 $PID 
	echo "$(date) Killed the process with PID: $PID NAME: $NAME"
    else
	echo "SIZE has not yet exceeding"
    fi
    
    sleep 10
done