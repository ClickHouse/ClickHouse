#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP TABLE IF EXISTS test_alter_custom_disk SYNC;

CREATE TABLE test_alter_custom_disk (a Int32, b Int64)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/local_alter/');

INSERT INTO test_alter_custom_disk SELECT 1, 2 FROM numbers(1);

ALTER TABLE test_alter_custom_disk ADD COLUMN c Int64 AFTER b;

SELECT * FROM test_alter_custom_disk;

DROP TABLE IF EXISTS test_alter_custom_disk SYNC;
"""
