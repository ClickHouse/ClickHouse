#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `text_index_serialization_version` setting is a preference, not a hard constraint: an index
# with `support_phrase_search` is always written in the 'v2_with_positions' format because older
# formats cannot represent positions. The `compatibility` setting may pin an older default on an
# existing table, but that must not make the index unwritable: inserts and merges keep working
# and keep writing the format the index requires, and readers take the format version from the
# on-disk header of each part.

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

echo '-- insert works under the pin'
$CLICKHOUSE_LOCAL --path "$data_path" --compatibility '26.5' -m -q "
INSERT INTO tab SELECT number + 1024, 'qux quux' FROM numbers(512);
SELECT count() FROM tab WHERE hasPhrase(str, 'qux quux');
"

echo '-- merge works under the pin'
$CLICKHOUSE_LOCAL --path "$data_path" --compatibility '26.5' -m -q "
OPTIMIZE TABLE tab FINAL;
SELECT count() FROM tab WHERE hasPhrase(str, 'foo bar');
SELECT count() FROM tab WHERE hasPhrase(str, 'qux quux');
"

echo '-- the part merged under the pin contains the positions substream'
part_path=$($CLICKHOUSE_LOCAL --path "$data_path" -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active")
if [ -f "${part_path}skp_idx_text_idx.pos.idx" ]; then
    echo 'positions file exists'
else
    echo "no positions file in $part_path:"
    ls "$part_path"
fi

echo '-- without the pin writes and merges keep working'
$CLICKHOUSE_LOCAL --path "$data_path" -m -q "
INSERT INTO tab SELECT number + 1536, 'corge grault' FROM numbers(512);
OPTIMIZE TABLE tab FINAL;
SELECT count() FROM tab WHERE hasPhrase(str, 'corge grault');
"

rm -rf "${data_path:?}"
