#!/usr/bin/env bash

if [ "$1" != '-n' ]
then
    echo 'DROP DATABASE IF EXISTS pre' | clickhouse-client || exit 1
    echo 'CREATE DATABASE pre' | clickhouse-client || exit 2
    create_query="CREATE TABLE pre.__NAME__ (d0 Date, key UInt64, i64 Int64, s String, d Date, dt DateTime, f32 Float32, fs11 FixedString(11), ars Array(String), arui8 Array(UInt8), n Nested(ui16 UInt16, s String)) ENGINE=__ENGINE__"
    insert_query="INSERT INTO pre.__NAME__ SELECT toDate('2014-01-01') AS d0, number AS key, toInt64(number + 11) AS i64, concat('upchk', toString(number * 2)) AS s, toDate(toUInt64(toDate('2014-01-01')) + number%(15*12) * 30) AS d, toDateTime(toUInt64(toDateTime('2014-01-01 00:00:00')) + number%(24*5000) * 3600) AS dt, toFloat32(number / 1048576) AS f32, toFixedString(concat('fix', toString(number * 3)), 11) AS fs11, arrayMap(x->concat('ars', toString(number + x)), arrayFilter(x->x<number%15, [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14])) AS ars, arrayMap(x->toUInt8(number * (x-3)), arrayFilter(x->x<=(number+1)%3, [0,1,2])) AS arui8, arrayMap(x->toUInt16((x+1)*number),arrayFilter(x->x<number*5%7,[0,1,2,3,4,5,6]) AS n) AS "'`'"n.ui16"'`'", arrayMap(x->toString((x+100)*number),n) AS "'`'"n.s"'`'" FROM system.numbers LIMIT 12345678"
    echo "$create_query" | sed "s/__ENGINE__/TinyLog/;s/__NAME__/b/" | clickhouse-client || exit 3
    echo "$create_query" | sed "s/__ENGINE__/MergeTree(d0, (key, key), 8192)/;s/__NAME__/t/" | clickhouse-client || exit 4
    echo "$insert_query" | sed "s/__NAME__/b/" | clickhouse-client || exit 5
    echo "$insert_query" | sed "s/__NAME__/t/" | clickhouse-client || exit 6
fi
# 4 mark ranges (1)
query1="SELECT * FROM pre.__NAME__ WHERE (key > 9000 AND key < 100000 OR key > 200000 AND key < 1000000 OR key > 3000000 AND key < 8000000 OR key > 12000000)"
# 4 mark ranges, 5 random rows from each mark; should have about 1638 times less rows than (1)
query2="SELECT * FROM pre.__NAME__ WHERE (key > 9000 AND key < 100000 OR key > 200000 AND key < 1000000 OR key > 3000000 AND key < 8000000 OR key > 12000000) AND i64 * (intDiv(i64, 8192) % 4096 * 2 + 1) % 8192 IN (4167, 6420, 1003, 5006, 321)"
# 4 mark ranges, 1/4 random marks; should have about 4 times less rows than (1)
query3="SELECT * FROM pre.__NAME__ WHERE (key > 9000 AND key < 100000 OR key > 200000 AND key < 1000000 OR key > 3000000 AND key < 8000000 OR key > 12000000) AND intHash32(intDiv(i64, 8192)) % 4 = 0"
# 4 mark ranges, 1/4 random marks; should have about 4096 times less rows than (1)
query4="SELECT * FROM pre.__NAME__ WHERE (key > 9000 AND key < 100000 OR key > 200000 AND key < 1000000 OR key > 3000000 AND key < 8000000 OR key > 12000000) AND intHash32(intDiv(i64, 8192)) % 4 = 0 AND i64 * (intDiv(i64, 8192) % 4096 * 2 + 1) % 8192 IN (2953, 6677, 8135, 2971, 2435, 1961, 5976, 3184)"

for i in {1..4}
do
    eval query=\$query$i
    echo "Query $i from TinyLog"
    time echo "$query" | sed "s/__NAME__/b/" | clickhouse-client > r${i}b || exit 7
    echo "Query $i from MergeTree with WHERE"
    time echo "$query" | sed "s/__NAME__/t/" | clickhouse-client > r${i}t || exit 8
    echo "Query $i from MergeTree with PREWHERE"
    time echo "$query" | sed "s/WHERE/PREWHERE/" | sed "s/__NAME__/t/" | clickhouse-client > r${i}p || exit 8
    sort r${i}b > r${i}bs
    sort r${i}t > r${i}ts
    sort r${i}p > r${i}ps
    diff -q r${i}bs r${i}ts
    if [ $? -ne 0 ]
    then
        echo "TinyLog and MergeTree with WHERE differ on query $i"
        exit 9
    fi
    diff -q r${i}bs r${i}ps
    if [ $? -ne 0 ]
    then
        echo "TinyLog and MergeTree with PREWHERE differ on query $i"
        exit 10
    fi
done

echo "Passed"

