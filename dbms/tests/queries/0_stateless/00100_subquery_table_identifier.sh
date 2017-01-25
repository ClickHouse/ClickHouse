#!/bin/sh

clickhouse-client --query="SELECT sum(dummy) FROM remote('localhost', system, one) WHERE 1 GLOBAL IN (SELECT 1)"
echo '1' | clickhouse-client --external --file=- --types=UInt8 --query="SELECT 1 IN _data"
echo '1' | clickhouse-client --external --file=- --types=UInt8 --query="SELECT 1 IN (SELECT * FROM _data)"
echo '1' | clickhouse-client --external --file=- --types=UInt8 --query="SELECT dummy FROM remote('localhost', system, one) WHERE 1 GLOBAL IN _data"
echo '1' | clickhouse-client --external --file=- --types=UInt8 --query="SELECT dummy FROM remote('localhost', system, one) WHERE 1 IN _data"
