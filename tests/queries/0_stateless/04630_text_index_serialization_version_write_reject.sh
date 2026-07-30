#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An index with `support_phrase_search` requires the 'v2_with_positions' serialization
# version. The combination of such an index with an older pinned version is rejected on
# CREATE/ALTER, but it can appear on an existing table when the `compatibility` setting
# pins an old default. In that case every attempt to write the index must be rejected
# with a clear error, while reads keep working (readers take the version from the
# on-disk header).

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
CREATE TABLE tab (id UInt32, str String, INDEX text_idx str TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1))
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1;
INSERT INTO tab SELECT number, 'foo bar' FROM numbers(512);
INSERT INTO tab SELECT number + 512, 'foo baz' FROM numbers(512);
SELECT count() FROM tab WHERE hasPhrase(str, 'foo bar');
"

# `compatibility` below the version that introduced positions resolves the table's effective
# `text_index_serialization_version` to 'v0_initial' without any change to the table metadata.

echo '-- reads work under the pin'
$CLICKHOUSE_LOCAL --path "$data_path" --compatibility '26.5' -q "SELECT count() FROM tab WHERE hasPhrase(str, 'foo bar')"

echo '-- insert is rejected under the pin'
$CLICKHOUSE_LOCAL --path "$data_path" --compatibility '26.5' -q "INSERT INTO tab SELECT number + 1024, 'qux quux' FROM numbers(512)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '-- merge is rejected under the pin'
$CLICKHOUSE_LOCAL --path "$data_path" --compatibility '26.5' -q "OPTIMIZE TABLE tab FINAL" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo '-- without the pin writes and merges work again'
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
INSERT INTO tab SELECT number + 1024, 'qux quux' FROM numbers(512);
OPTIMIZE TABLE tab FINAL;
SELECT count() FROM tab WHERE hasPhrase(str, 'qux quux');
"

rm -rf "${data_path:?}"
