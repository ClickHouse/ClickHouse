#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Substrait is not built in fast test

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Of each exception only the message is printed.
$CLICKHOUSE_CLIENT --enable_substrait 1 --ignore-error -q "
    DROP TABLE IF EXISTS t_substrait_blocked;
    CREATE TABLE t_substrait_blocked (id Int64, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_substrait_blocked VALUES (1, 'a'), (2, 'b');

    SELECT 'A step without a Substrait representation';
    EXPLAIN SUBSTRAIT SELECT id FROM t_substrait_blocked LIMIT 1 BY name;

    SELECT 'Every blocked branch is reported, left branch first';
    EXPLAIN SUBSTRAIT SELECT cityHash64(name) FROM t_substrait_blocked UNION ALL SELECT sipHash64(name) FROM t_substrait_blocked;

    DROP TABLE t_substrait_blocked;
" 2>&1 | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: .*DB::Exception: //; s/\. \(NOT_IMPLEMENTED\)$//'
