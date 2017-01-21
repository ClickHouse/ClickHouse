#!/usr/bin/env bash

# См. таску CONV-8849.
# Симулируем ситуацию, когда половина одного файла с засечками не успела записаться на диск

path='/opt/clickhouse/data/mergetest/a/'

echo 'Creating table'
echo 'CREATE DATABASE IF NOT EXISTS mergetest' | clickhouse-client || exit 1
echo 'DROP TABLE IF EXISTS mergetest.a' | clickhouse-client || exit 2
echo 'CREATE TABLE mergetest.a (d Date, x String) ENGINE=MergeTree(d, tuple(x), 8192)' | clickhouse-client || exit 3

echo 'Inserting data'
echo 'INSERT INTO mergetest.a SELECT toDate('2013-10-01') AS d, toString(number) AS x FROM system.numbers LIMIT 5000000' | clickhouse-client || exit 4

echo 'Files:'
ls $path

echo 'Selecting data'
echo "SELECT * FROM mergetest.a WHERE x > '4000000'" | clickhouse-client > temp_data1 || exit 5

echo 'Calling OPTIMIZE many times'
for i in {1..50}
do
  echo 'OPTIMIZE TABLE mergetest.a' | clickhouse-client || exit 6
done

echo 'Files (there should be few non-old_ pieces):'
ls $path

echo 'Stopping server'
sudo /etc/init.d/clickhouse-server stop || exit 7

echo 'Truncating in half each non-old_ piece with level>1'
pieces=`ls $path | grep -Pv '(^tmp_|^old_|_0$)' | grep -v 'increment.txt'` || exit 8
for piece in $pieces
do
  mrkfile="$path/$piece"/x.mrk
  ((s=`stat -c'%s' "$mrkfile"`/2)) || exit 9
  echo "Truncating $mrkfile to $s bytes"
  truncate -s $s "$mrkfile" || exit 10
done

echo 'Starting server'
sudo /etc/init.d/clickhouse-server start || exit 11

until echo 'SHOW DATABASES' | clickhouse-client > /dev/null
do
  echo 'Waiting for server to start'
  sleep 2
done

echo 'Files:'
ls $path

echo 'Selecting data'
echo "SELECT * FROM mergetest.a WHERE x > '4000000'" | clickhouse-client > temp_data2 || exit 12

sort temp_data1 > temp_data1s
sort temp_data2 > temp_data2s

if diff -q temp_data1s temp_data2s
then
  echo 'Everything is fine, nothing is broken'
else
  echo 'Everything is broken :('
fi

