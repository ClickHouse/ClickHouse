#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An ALIAS column calls a SQL UDF whose body names a dictionary without a database. Reading the column from
# another current database and the mutation of a lightweight DELETE have to find the dictionary (issue #123489).

# A SQL UDF is a server-wide object, so its name has to be unique across concurrently running tests.
UDF="${CLICKHOUSE_DATABASE}_udf_dict"

$CLICKHOUSE_CLIENT --query "
CREATE DICTIONARY d (id UInt64, v UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(QUERY 'SELECT toUInt64(1) AS id, toUInt64(42) AS v'))
LAYOUT(FLAT())
LIFETIME(0);

CREATE FUNCTION ${UDF} AS x -> dictGet(d, 'v', x);

CREATE TABLE t (id UInt64, v ALIAS ${UDF}(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t (id) VALUES (1), (2);

SELECT id, v FROM t ORDER BY id;
"

$CLICKHOUSE_CLIENT --query "
USE system;
SELECT id, v FROM ${CLICKHOUSE_DATABASE}.t ORDER BY id;
"

$CLICKHOUSE_CLIENT --query "
DELETE FROM t WHERE id = 1;
SELECT id, v FROM t ORDER BY id;
"

$CLICKHOUSE_CLIENT --query "
DROP TABLE t;
DROP FUNCTION ${UDF};
DROP DICTIONARY d;
"
