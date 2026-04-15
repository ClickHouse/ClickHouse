#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -nm -q "
    DROP TABLE IF EXISTS 04099_data_src, 04099_data_tgt;

    CREATE TABLE 04099_data_src (key UInt32, val Nullable(String))
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 1,
            string_serialization_version = 'single_stream',
            serialization_info_version = 'with_types',
            nullable_serialization_version = 'basic',
            ratio_of_defaults_for_sparse_serialization = 0.93;

    CREATE TABLE 04099_data_tgt (key UInt32, val Nullable(String))
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 1,
            string_serialization_version = 'with_size_stream',
            serialization_info_version = 'with_types',
            nullable_serialization_version = 'basic',
            ratio_of_defaults_for_sparse_serialization = 0.93;

    INSERT INTO 04099_data_src SELECT number, 'val-' || number FROM numbers(100);

    ALTER TABLE 04099_data_src MOVE PARTITION tuple() TO TABLE 04099_data_tgt;
    ALTER TABLE 04099_data_tgt MODIFY COLUMN val String DEFAULT '' SETTINGS mutations_sync = 2;

    SELECT * FROM 04099_data_tgt ORDER BY key LIMIT 5;

    DROP TABLE 04099_data_src, 04099_data_tgt;
"
