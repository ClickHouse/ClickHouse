#!/usr/bin/env bash

if [ "$1" = "d" ]
then
    tmuxgdb -ex=start -args ./build-dev/dbms/programs/clickhouse server --config etc/config-dev.xml
else
    ./build-dev/dbms/programs/clickhouse server --config etc/config-dev.xml
fi
