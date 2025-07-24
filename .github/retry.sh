#!/bin/bash
# Execute command until exitcode is 0 or
# maximum number of retries is reached
# Example:
#    ./retry <retries> <delay> <command>
retries=$1
delay=$2
command="${@:3}"
exitcode=0
try=0
until [ "$try" -ge $retries ]
do
  echo "$command"
  eval "$command"
  exitcode=$?
  if [ $exitcode -eq 0 ]; then
    break
  fi
  try=$((try+1))
  sleep $2
done
exit $exitcode
