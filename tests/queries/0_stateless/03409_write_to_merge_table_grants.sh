#!/usr/bin/env bash
# Tag no-parallel: create user

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_USER="user_03409_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT --multiline -q """
    CREATE TABLE test03409_table_1 (key UInt32) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test03409_table_2 (key UInt32) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test03409_table_3 (key UInt32) Engine=MergeTree() ORDER BY key;
    CREATE TABLE test03409_merge_ro (key UInt32) Engine=Merge($CLICKHOUSE_DATABASE, 'test03409_table_\d+');
    CREATE TABLE test03409_merge_rw (key UInt32) Engine=Merge($CLICKHOUSE_DATABASE, 'test03409_table_\d+', 'test03409_table_1');
    CREATE TABLE test03409_merge_auto (key UInt32) Engine=Merge($CLICKHOUSE_DATABASE, 'test03409_table_\d+', auto);
    CREATE TABLE test03409_merge_no_table (key UInt32) Engine=Merge($CLICKHOUSE_DATABASE, 'test03409_notable_\d+', 'test03409_notable_1');
    CREATE TABLE test03409_merge_no_table_auto (key UInt32) Engine=Merge($CLICKHOUSE_DATABASE, 'test03409_notable_\d+', auto);
    DROP USER IF EXISTS $CLICKHOUSE_USER;
    CREATE USER $CLICKHOUSE_USER IDENTIFIED WITH plaintext_password BY 'user_03409_password';
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_merge_ro TO $CLICKHOUSE_USER;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_merge_rw TO $CLICKHOUSE_USER;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_merge_auto TO $CLICKHOUSE_USER;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_merge_no_table TO $CLICKHOUSE_USER;
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_merge_no_table_auto TO $CLICKHOUSE_USER;
"""

$CLICKHOUSE_CLIENT --multiline --user $CLICKHOUSE_USER --password user_03409_password -q """
    INSERT INTO test03409_merge_ro VALUES (1); -- { serverError TABLE_IS_READ_ONLY }
    INSERT INTO test03409_merge_rw VALUES (2); -- { serverError ACCESS_DENIED }
    INSERT INTO test03409_merge_auto VALUES (3); -- { serverError ACCESS_DENIED }
    INSERT INTO test03409_merge_no_table VALUES (4); -- { serverError ACCESS_DENIED }
    INSERT INTO test03409_merge_no_table_auto VALUES (5); -- { serverError UNKNOWN_TABLE }
"""

$CLICKHOUSE_CLIENT --multiline -q """
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_table_2 TO $CLICKHOUSE_USER;
"""

$CLICKHOUSE_CLIENT --multiline --user $CLICKHOUSE_USER --password user_03409_password -q """
    INSERT INTO test03409_merge_ro VALUES (6); -- { serverError TABLE_IS_READ_ONLY }
    INSERT INTO test03409_merge_rw VALUES (7); -- { serverError ACCESS_DENIED }
    INSERT INTO test03409_merge_auto VALUES (8);
"""

$CLICKHOUSE_CLIENT --multiline -q """
    GRANT INSERT ON $CLICKHOUSE_DATABASE.test03409_table_1 TO $CLICKHOUSE_USER;
"""

$CLICKHOUSE_CLIENT --multiline --user $CLICKHOUSE_USER --password user_03409_password -q """
    INSERT INTO test03409_merge_ro VALUES (9); -- { serverError TABLE_IS_READ_ONLY }
    INSERT INTO test03409_merge_rw VALUES (10);
    INSERT INTO test03409_merge_auto VALUES (11);
"""

$CLICKHOUSE_CLIENT --multiline -q """
    OPTIMIZE TABLE test03409_table_1 FINAL;
    OPTIMIZE TABLE test03409_table_2 FINAL;
    OPTIMIZE TABLE test03409_table_3 FINAL;
    SELECT '*** Select from test03409_table_1 ***';
    SELECT * FROM test03409_table_1 ORDER BY key FORMAT CSV;
    SELECT '*** Select from test03409_table_2 ***';
    SELECT * FROM test03409_table_2 ORDER BY key FORMAT CSV;
    SELECT '*** Select from test03409_table_3 ***';
    SELECT * FROM test03409_table_3 ORDER BY key FORMAT CSV;
"""
