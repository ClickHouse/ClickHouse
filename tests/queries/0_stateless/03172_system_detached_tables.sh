#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATABASE_ATOMIC="${CLICKHOUSE_DATABASE}_atomic"
DATABASE_LAZY="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT "

SELECT 'database atomic tests';
DROP DATABASE IF EXISTS ${DATABASE_ATOMIC};
CREATE DATABASE IF NOT EXISTS ${DATABASE_ATOMIC} ENGINE=Atomic;

CREATE TABLE ${DATABASE_ATOMIC}.test_table (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

DETACH TABLE ${DATABASE_ATOMIC}.test_table;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

ATTACH TABLE ${DATABASE_ATOMIC}.test_table;

CREATE TABLE ${DATABASE_ATOMIC}.test_table_perm (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

DETACH TABLE ${DATABASE_ATOMIC}.test_table_perm PERMANENTLY;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

DETACH TABLE ${DATABASE_ATOMIC}.test_table SYNC;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}';

SELECT database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_ATOMIC}' AND table='test_table';

DROP DATABASE ${DATABASE_ATOMIC} SYNC;

"

$CLICKHOUSE_CLIENT "

SELECT '-----------------------';
SELECT 'database lazy tests';

DROP DATABASE IF EXISTS ${DATABASE_LAZY};
CREATE DATABASE ${DATABASE_LAZY} Engine=Lazy(10);

CREATE TABLE ${DATABASE_LAZY}.test_table (number UInt64) engine=Log;
INSERT INTO ${DATABASE_LAZY}.test_table SELECT * FROM numbers(100);
DETACH TABLE ${DATABASE_LAZY}.test_table;

CREATE TABLE ${DATABASE_LAZY}.test_table_perm (number UInt64) engine=Log;
INSERT INTO ${DATABASE_LAZY}.test_table_perm SELECT * FROM numbers(100);
DETACH table ${DATABASE_LAZY}.test_table_perm PERMANENTLY;

SELECT 'before attach', database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_LAZY}';

ATTACH TABLE ${DATABASE_LAZY}.test_table;
ATTACH TABLE ${DATABASE_LAZY}.test_table_perm;

SELECT 'after attach', database, table, is_permanently FROM system.detached_tables WHERE database='${DATABASE_LAZY}';

SELECT 'DROP TABLE';
DROP TABLE  ${DATABASE_LAZY}.test_table SYNC;
DROP TABLE  ${DATABASE_LAZY}.test_table_perm SYNC;

DROP DATABASE ${DATABASE_LAZY} SYNC;

SELECT '-----------------------';
SELECT 'detached table no loop';

SET max_block_size = 8;
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t1 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t2 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t3 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t4 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t5 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t6 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t7 (c0 Int) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t8 (c0 Int) ENGINE = MergeTree ORDER BY c0;
DROP TABLE t0;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;
SELECT 1 FROM system.detached_tables;

"
