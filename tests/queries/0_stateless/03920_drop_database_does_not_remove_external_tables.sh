#!/usr/bin/env bash
# Tags: no-flaky-check

# Verify that DROP DATABASE does not remove tables from other databases,
# even when tables inside the dropped database have loading dependencies on external tables.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --multiquery "
    CREATE DATABASE db_to_drop;
    CREATE DATABASE db_to_keep;

    CREATE TABLE db_to_keep.join_table (id UInt64, value String) ENGINE = Join(ANY, LEFT, id);
    INSERT INTO db_to_keep.join_table VALUES (1, 'hello'), (2, 'world');

    -- This table has a loading dependency on db_to_keep.join_table via joinGet.
    CREATE TABLE db_to_drop.dependent (id UInt64, value String DEFAULT joinGet('db_to_keep.join_table', 'value', id)) ENGINE = MergeTree ORDER BY id;
    INSERT INTO db_to_drop.dependent (id) VALUES (1), (2);

    SELECT * FROM db_to_drop.dependent ORDER BY id;

    DROP DATABASE db_to_drop;

    -- The table in the other database must still exist.
    SELECT * FROM db_to_keep.join_table ORDER BY id;
"
