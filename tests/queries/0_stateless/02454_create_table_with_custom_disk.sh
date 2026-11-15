#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --multiline -q """
DROP TABLE IF EXISTS test;

EXPLAIN SYNTAX
CREATE TABLE test (a Int32)
ENGINE = MergeTree() order by tuple()
SETTINGS disk = disk(type=local, path='not a real path');

CREATE TABLE test (a Int32)
ENGINE = MergeTree() order by tuple()
SETTINGS disk = disk(type=local, path='/local/'); -- { serverError BAD_ARGUMENTS }

CREATE TABLE test (a Int32)
ENGINE = MergeTree() order by tuple()
SETTINGS disk = disk(type=local, path='${CLICKHOUSE_DISKS_FILES}/local/');

INSERT INTO test SELECT number FROM numbers(100);
SELECT count() FROM test;

DETACH TABLE test;
ATTACH TABLE test;

SHOW CREATE TABLE test;
DESCRIBE TABLE test;

INSERT INTO test SELECT number FROM numbers(100);
SELECT count() FROM test;
"""
