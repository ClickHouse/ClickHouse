#!/bin/bash -e

# Скрипт довольно хрупкий.

# Попробуем угадать, где расположен конфиг сервера.
[ -f '/etc/clickhouse-server/config-preprocessed.xml' ] && CONFIG='/etc/clickhouse-server/config-preprocessed.xml' || CONFIG='../src/Server/config-preprocessed.xml'

if [ ! -f "$CONFIG" ]; then
	echo "Cannot find config file for clickhouse-server" >&2
	exit 1
fi

# Создадим директории для данных второго сервера.
PATH2=/tmp/clickhouse/
[ -d "$PATH2" ] && rm -rf $PATH2
mkdir -p ${PATH2}{data,metadata}/default/

# Создадим второй конфиг с портом 9001.
CONFIG2="config-9001.xml"

LOG=${PATH2}log

cat "$CONFIG" | sed -r \
	's/<path>.+<\/path>/<path>'${PATH2//\//\\/}'<\/path>/;
	 s/<http_port>[0-9]+/<http_port>8124/;
	 s/<tcp_port>[0-9]+/<tcp_port>9001/;
	 s/<interserver_http_port>[0-9]+/<interserver_http_port>9010/;
	 s/users\.xml/users-preprocessed.xml/' > $CONFIG2

cp ${CONFIG/config/users} .

# Запустим второй сервер.
BINARY=$(readlink /proc/$(pidof clickhouse-server | tr ' ' '\n' | head -n1)/exe || echo "/usr/bin/clickhouse-server")

if [ ! -x "$BINARY" ] && [ -n "$(pgrep memcheck)" ]; then
	# В случае, если сервер был запущен под valgrind-ом.
	BINARY=$(readlink /proc/$(pgrep memcheck)/cwd)/$(cat /proc/$(pgrep memcheck)/cmdline | cut -f2 -d '')
fi

if [ ! -x "$BINARY" ]; then
	echo "Cannot find executable binary for running clickhouse-server" >&2
	exit 1
fi

$BINARY --config-file=${CONFIG2} 2>$LOG &
PID=$!

function finish {
	kill $PID
	wait
}

trap finish EXIT

i=0
while true; do
	grep -q 'Ready for connections' ${LOG} && break
	grep -q 'shutting down' ${LOG} && echo "Cannot start second clickhouse-server" && exit 1
	sleep 0.05

	i=$(($i + 1))
	[[ $i == 100 ]] && echo "Cannot start second clickhouse-server" && exit 1
done

rm "$CONFIG2"


# Теперь можно выполнять запросы.

CLIENT1='clickhouse-client --port=9000'
CLIENT2='clickhouse-client --port=9001'


$CLIENT1 -n --query="
	CREATE DATABASE IF NOT EXISTS test;
	DROP TABLE IF EXISTS test.half1;
	DROP TABLE IF EXISTS test.half2;
	CREATE TABLE test.half1 ENGINE = Memory AS SELECT number FROM system.numbers LIMIT 5;
	CREATE TABLE test.half2 ENGINE = Memory AS SELECT number FROM system.numbers WHERE number % 2 = 0 LIMIT 5;
	"

$CLIENT2 -n --query="
	CREATE DATABASE IF NOT EXISTS test;
	DROP TABLE IF EXISTS test.half1;
	DROP TABLE IF EXISTS test.half2;
	CREATE TABLE test.half1 ENGINE = Memory AS SELECT number FROM system.numbers LIMIT 5, 5;
	CREATE TABLE test.half2 ENGINE = Memory AS SELECT number FROM system.numbers WHERE number % 2 = 1 LIMIT 5;
	"

$CLIENT1 -n --query="
	SELECT count() FROM remote('localhost:{9000,9001}', test, half1)
		WHERE number IN (SELECT * FROM test.half2);

	SELECT count() FROM remote('localhost:{9000,9001}', test, half1)
		WHERE number IN (SELECT * FROM remote('localhost:{9000,9001}', test, half2));

	SELECT count() FROM remote('localhost:{9000,9001}', test, half1)
		WHERE number GLOBAL IN (SELECT * FROM remote('localhost:{9000,9001}', test, half2));
	"

$CLIENT1 -n --query="
	DROP TABLE test.half1;
	DROP TABLE test.half2;"

$CLIENT2 -n --query="
	DROP TABLE test.half1;
	DROP TABLE test.half2;"
