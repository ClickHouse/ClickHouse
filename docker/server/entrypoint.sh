#!/usr/bin/env sh

userdel clickhouse 2>/dev/null
groupdel clickhouse 2>/dev/null
groupadd -g $USER_GID clickhouse
useradd -d /home/clickhouse -g clickhouse -u $USER_UID -s /nonexistent clickhouse

chown -R clickhouse /etc/clickhouse-server
chown -R clickhouse /var/log/clickhouse-server
chown -R clickhouse /var/lib/clickhouse

gosu clickhouse /usr/bin/clickhouse-server --config=${CLICKHOUSE_CONFIG} &
child=$!

trap "kill $child" INT TERM
wait "$child"
trap - INT TERM
wait "$child"
