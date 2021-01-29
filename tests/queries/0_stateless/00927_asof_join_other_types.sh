#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for typename in "UInt32" "UInt64" "Float64" "Float32" "DateTime" "Decimal32(5)" "Decimal64(5)" "Decimal128(5)" "DateTime64(3)"
do
    $CLICKHOUSE_CLIENT -mn <<EOF
DROP TABLE IF EXISTS A;
DROP TABLE IF EXISTS B;

CREATE TABLE A(k UInt32, t ${typename}, a Float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO A(k,t,a) VALUES (2,1,1),(2,3,3),(2,5,5);

CREATE TABLE B(k UInt32, t ${typename}, b Float64) ENGINE = MergeTree() ORDER BY (k, t);
INSERT INTO B(k,t,b) VALUES (2,3,3);

SELECT k, t, a, b FROM A ASOF LEFT JOIN B USING(k,t) ORDER BY (k,t);

DROP TABLE A;
DROP TABLE B;
EOF

done