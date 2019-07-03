#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo 'select 1' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL}/?max_query_size=8 -d @- 2>&1 | grep -o "Max query size exceeded"
echo -
echo 'select 1' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL}/?max_query_size=7 -d @- 2>&1 | grep -o "Max query size exceeded"

echo "select '1'" | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL}/?max_query_size=10 -d @- 2>&1 | grep -o "Max query size exceeded"
echo -
echo "select '11'" | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL}/?max_query_size=10 -d @- 2>&1 | grep -o "Max query size exceeded"

echo 'drop table if exists tab_00612_1' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL_PARAMS} -d @-
echo 'create table tab_00612_1 (key UInt64, val UInt64) engine = MergeTree order by key' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL_PARAMS} -d @-
echo 'into tab_00612_1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL_PARAMS}&max_query_size=30&query=insert" -d @-
echo 'select val from tab_00612_1 order by val' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL_PARAMS} -d @-
echo 'drop table tab_00612_1' | ${CLICKHOUSE_CURL} -sSg ${CLICKHOUSE_URL_PARAMS} -d @-

echo "
import requests
import os

url = os.environ['CLICKHOUSE_URL']
if not url.startswith('http'):
    url = 'http://' + url
q = 'select sum(number) from (select * from system.numbers limit 10000000) where number = 0'

def gen_data(q):
    yield q
    yield ''.join([' '] * (1024 - len(q)))

    pattern = ''' or toString(number) = '{}'\n'''

    for i in range(1, 4 * 1024):
        yield pattern.format(str(i).zfill(1024 - len(pattern) + 2))

s = requests.Session()
resp = s.post(url + '/?max_query_size={}'.format(1 << 21), timeout=1, data=gen_data(q), stream=True,
              headers = {'Connection': 'close'})

for line in resp.iter_lines():
    print line
" | python | grep -o "Max query size exceeded"
echo -

