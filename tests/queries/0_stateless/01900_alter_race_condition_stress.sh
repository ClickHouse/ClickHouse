#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

N=100
exitcode=0

for i in $(seq 1 $N)
do
   ${CURDIR}/01900_alter_race_condition.sh > ${CURDIR}/01900_alter_race_condition_${i}.stdout 2>&1 &
done

wait

for i in $(seq 1 $N)
do
   diff ${CURDIR}/01900_alter_race_condition_${i}.stdout ${CURDIR}/01900_alter_race_condition.reference > /dev/null 2>&1
   rc=$?
   if [ $rc -ne 0 ] && [ $exitcode -eq 0 ]; then
      diff -u ${CURDIR}/01900_alter_race_condition_${i}.stdout ${CURDIR}/01900_alter_race_condition.reference > ${CURDIR}/01900_alter_race_condition.diff
      exitcode=1
   fi
   rm -rf ${CURDIR}/01900_alter_race_condition_${i}.stdout
done

if [ $exitcode -ne 0 ]; then
   cat ${CURDIR}/01900_alter_race_condition.diff
   echo "FAIL"
   exit 1
fi

echo "OK"
exit 0
