#!/bin/bash

if [ -f /etc/clickhouse-server/conf.d/container_maintanence_mode.flag ]; then
   echo "Starting container in maintanence mode. It will sleep unless you shutdown it"
   sleep infinity
else
    chown -R clickhouse:clickhouse /jbod1 /jbod2 /external
    /test_setup/wait-for-it.sh zookeeper:2181 --timeout=0 --strict -- /entrypoint.sh
fi