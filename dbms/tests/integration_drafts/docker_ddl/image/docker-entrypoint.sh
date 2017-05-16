#!/bin/bash
set -e

echo "
<yandex>
    <macros>
        <layer>${LAYER}</layer>
        <shard>${SHARD}</shard>
        <replica>${REPLICA}</replica>
    </macros>
</yandex>" > /etc/metrika.xml

ls -l /etc/clickhouse-server/
ls -l /etc/clickhouse-server/conf.d/
ls -l /etc/clickhouse-server/users.d/

service clickhouse-server start

PID=`pidof clickhouse-server`
while [ -e /proc/$PID ]; do
    sleep 2
done
echo "ClickHouse process $PID has finished"
