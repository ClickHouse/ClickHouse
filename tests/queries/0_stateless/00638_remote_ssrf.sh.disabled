#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

user=readonly
address=${CLICKHOUSE_HOST}
port=${CLICKHOUSE_PORT_HTTP}
url="${CLICKHOUSE_PORT_HTTP_PROTO}://$address:$port/"

# Port is arbitary

# nc does not support -w option with -l and will wait forever for first packet
nc -l -p 61845 -q 0 > /dev/null &

${CLICKHOUSE_CURL} -sS $url --data-binary "SELECT * FROM remote('localhost:61845', system.one, 'user', 'password')" > /dev/null 2>&1

# Send packet to close listening nc (if clickhouse fails to send).
( echo -e "Finish him\n" | nc localhost 61845 ) 2>/dev/null || true

wait

nc -l -p 61846 -q 0 > /dev/null &

${CLICKHOUSE_CURL} -sS $url --data-binary "SELECT * FROM remote('localhost:61846', system.one, 'user', 'passw
ord')" 2>&1 | grep -o 'must not contain ASCII control characters'

( echo -e "Finish him\n" | nc localhost 61846 ) 2>/dev/null || true

wait
