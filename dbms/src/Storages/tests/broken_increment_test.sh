#!/usr/bin/env bash

# См. таску METR-9006
# Удалим increment.txt из StorageMergeTree таблицы и попробуем сделать INSERT в нее. Перезапустим сервер и попробуем сделать INSERT снова.

echo 'Droping database'
echo 'DROP DATABASE IF EXISTS increment' | clickhouse-client || exit 1
echo 'Creating database'
echo 'CREATE DATABASE increment' | clickhouse-client || exit 2
echo 'Creating table'
echo 'CREATE TABLE increment.a (d Date, v UInt64) ENGINE=MergeTree(d, tuple(v), 8192)' | clickhouse-client || exit 3
echo 'Inserting'
echo "2014-01-01	42" | clickhouse-client --query="INSERT INTO increment.a FORMAT TabSeparated" || exit 4
ls /var/lib/clickhouse/data/increment/a/
cat /var/lib/clickhouse/data/increment/a/increment.txt
rm /var/lib/clickhouse/data/increment/a/increment.txt
echo 'Inserting without increment.txt'
echo "2014-01-01	41" | clickhouse-client --query="INSERT INTO increment.a FORMAT TabSeparated"
ls /var/lib/clickhouse/data/increment/a/
cat /var/lib/clickhouse/data/increment/a/increment.txt
sudo service clickhouse-server stop
sudo service clickhouse-server start
sleep 10s
ls /var/lib/clickhouse/data/increment/a/
cat /var/lib/clickhouse/data/increment/a/increment.txt
echo 'Inserting after restart without increment.txt'
echo "2014-01-01	43" | clickhouse-client --query="INSERT INTO increment.a FORMAT TabSeparated"
ls /var/lib/clickhouse/data/increment/a/
cat /var/lib/clickhouse/data/increment/a/increment.txt
echo "SELECT * FROM increment.a" | clickhouse-client
