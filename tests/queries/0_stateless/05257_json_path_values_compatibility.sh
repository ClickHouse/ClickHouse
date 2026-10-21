#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `jsonPathValues` indexes need the 'v3_with_tokenizer_config' format. The `compatibility` setting
# lowers the effective `text_index_serialization_version` of a table ('v2_with_positions' for 26.8,
# 'v0_initial' for 26.5), but that must not make the index unwritable: create, insert, merge and
# reads keep working because the index raises the version to the one it requires.

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

for compatibility in '26.8' '26.5'; do
    echo "-- compatibility = ${compatibility}"
    rm -rf "${data_path:?}"

    echo '-- create and insert under the pin'
    $CLICKHOUSE_LOCAL --path "$data_path" --compatibility "$compatibility" -m -q "
    SET enable_json_type = 1;
    CREATE TABLE tab
    (
        id UInt64,
        data JSON(a String, b String),
        INDEX idx data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 1;
    INSERT INTO tab VALUES (1, '{\"a\":\"foo\"}'), (2, '{\"a\":\"bar\"}');
    INSERT INTO tab VALUES (3, '{\"a\":\"foo\"}'), (4, '{\"b\":\"foo\"}');
    SELECT arraySort(groupArray(id)) FROM tab WHERE data.a = 'foo' SETTINGS force_data_skipping_indices = 'idx';
    SELECT value FROM system.merge_tree_settings WHERE name = 'text_index_serialization_version';
    "

    echo '-- merge under the pin'
    $CLICKHOUSE_LOCAL --path "$data_path" --compatibility "$compatibility" -m -q "
    OPTIMIZE TABLE tab FINAL;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;
    SELECT arraySort(groupArray(id)) FROM tab WHERE data.a = 'foo' SETTINGS force_data_skipping_indices = 'idx';
    "

    echo '-- read without the pin'
    $CLICKHOUSE_LOCAL --path "$data_path" -m -q "
    SELECT arraySort(groupArray(id)) FROM tab WHERE data.b = 'foo' SETTINGS force_data_skipping_indices = 'idx';
    "
done

rm -rf "${data_path:?}"
