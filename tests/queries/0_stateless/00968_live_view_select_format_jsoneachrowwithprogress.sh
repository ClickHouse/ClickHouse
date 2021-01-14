#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<EOF 2>&1 | sed -r -e 's/"elapsed_time":"[0-9]+"/"elapsed_time":"<ELAPSED-TIME>"/'
SET allow_experimental_live_view = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS mt;

CREATE TABLE mt (a Int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT * FROM mt;

INSERT INTO mt VALUES (1),(2),(3);

SELECT * FROM lv FORMAT JSONEachRowWithProgress;

DROP TABLE lv;
DROP TABLE mt;
EOF
