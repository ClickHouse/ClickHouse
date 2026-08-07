#!/usr/bin/env bash
# Tags: stateful, no-parallel-replicas, no-object-storage
# The row ordering is not guaranteed with parallel replicas and a limit

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

model=$(mktemp "$CLICKHOUSE_TMP/obfuscator-model-XXXXXX.bin")

$CLICKHOUSE_CLIENT --max_threads 1 --query="SELECT URL, Title, SearchPhrase FROM test.hits LIMIT 1000" > "${CLICKHOUSE_TMP}"/data.tsv

$CLICKHOUSE_OBFUSCATOR --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --seed hello --limit 0 --save "$model" < "${CLICKHOUSE_TMP}"/data.tsv 2>/dev/null
wc -c < "$model"
$CLICKHOUSE_OBFUSCATOR --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --seed hello --limit 2500 --load "$model" < "${CLICKHOUSE_TMP}"/data.tsv > "${CLICKHOUSE_TMP}"/data2500.tsv 2>/dev/null
rm "$model"

$CLICKHOUSE_LOCAL --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --query "SELECT count(), uniq(URL), uniq(Title), uniq(SearchPhrase) FROM table" < "${CLICKHOUSE_TMP}"/data.tsv
$CLICKHOUSE_LOCAL --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --query "SELECT count(), uniq(URL), uniq(Title), uniq(SearchPhrase) FROM table" < "${CLICKHOUSE_TMP}"/data2500.tsv

rm "${CLICKHOUSE_TMP}"/data.tsv
rm "${CLICKHOUSE_TMP}"/data2500.tsv
