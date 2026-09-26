#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY="SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*\$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM url('https://clickhouse-public-datasets-eu-west-1.s3.amazonaws.com/hits_1t/0002.parquet') WHERE Referer != '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25"

TERM=xterm ${CLICKHOUSE_FORMAT} --hilite --query "$QUERY"

TERM=xterm ${CLICKHOUSE_FORMAT} --highlight --query "$QUERY"
