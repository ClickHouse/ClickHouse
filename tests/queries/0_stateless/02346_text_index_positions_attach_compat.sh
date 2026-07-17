#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: text index is not built in fast test.

# Regression for the upgrade-compatibility break from #109900, which renamed the
# MergeTree setting `allow_experimental_text_index_positions` and the text-index
# argument `positions` without a compatibility alias. A table written by an older
# server references the old names, and a newer server must still attach it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tab"

$CLICKHOUSE_CLIENT --query "
    SET allow_experimental_full_text_index = 1;
    CREATE TABLE tab
    (
        id UInt64,
        message String,
        INDEX idx (message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS allow_experimental_text_index_phrase_search = 1;
    INSERT INTO tab VALUES (1, 'hello world');
"

metadata_path=$($CLICKHOUSE_CLIENT --query "SELECT metadata_path FROM system.tables WHERE table = 'tab' AND database = currentDatabase()")
data_path=$($CLICKHOUSE_CLIENT --query "SELECT path FROM system.disks WHERE name = 'default'")

$CLICKHOUSE_CLIENT --query "DETACH TABLE tab"

# Rewrite metadata to the pre-#109900 names, as an older server would have written it.
sed -i -e 's/support_phrase_search = 1/positions = 1/' \
       -e 's/allow_experimental_text_index_phrase_search = 1/allow_experimental_text_index_positions = 1/' \
       "$data_path$metadata_path"

# ATTACH must tolerate the obsolete setting and the legacy `positions` argument.
$CLICKHOUSE_CLIENT --query "ATTACH TABLE tab"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM tab"

$CLICKHOUSE_CLIENT --query "DROP TABLE tab"
