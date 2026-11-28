#!/usr/bin/env bash
# Tags: stateful, no-parallel-replicas, no-object-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

model=$(mktemp "$CLICKHOUSE_TMP/obfuscator-model-XXXXXX.bin")

# Compared to explicitly specifying the structure of the input,
#  schema inference adds Nullable(T) to all types, so the model and the results
#  are a bit different from test '00175_obfuscator_schema_inference.sh'

$CLICKHOUSE_CLIENT --max_threads 1 --query="SELECT URL, Title, SearchPhrase FROM test.hits LIMIT 1000" > "${CLICKHOUSE_TMP}"/data.tsv

# Test obfuscator without saving the model
$CLICKHOUSE_OBFUSCATOR --input-format TSV --output-format TSV --seed hello --limit 2500 < "${CLICKHOUSE_TMP}"/data.tsv > "${CLICKHOUSE_TMP}"/data2500.tsv 2>/dev/null

# Test obfuscator with saving the model
$CLICKHOUSE_OBFUSCATOR --input-format TSV --output-format TSV --seed hello --limit 0 --save "$model" < "${CLICKHOUSE_TMP}"/data.tsv 2>/dev/null
wc -c < "$model"
$CLICKHOUSE_OBFUSCATOR --input-format TSV --output-format TSV --seed hello --limit 2500 --load "$model" < "${CLICKHOUSE_TMP}"/data.tsv > "${CLICKHOUSE_TMP}"/data2500_load_from_model.tsv 2>/dev/null
rm "$model"

$CLICKHOUSE_LOCAL --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --query "SELECT count(), uniq(URL), uniq(Title), uniq(SearchPhrase) FROM table" < "${CLICKHOUSE_TMP}"/data.tsv
$CLICKHOUSE_LOCAL --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --query "SELECT count(), uniq(URL), uniq(Title), uniq(SearchPhrase) FROM table" < "${CLICKHOUSE_TMP}"/data2500.tsv
$CLICKHOUSE_LOCAL --structure "URL String, Title String, SearchPhrase String" --input-format TSV --output-format TSV --query "SELECT count(), uniq(URL), uniq(Title), uniq(SearchPhrase) FROM table" < "${CLICKHOUSE_TMP}"/data2500_load_from_model.tsv

rm "${CLICKHOUSE_TMP}"/data.tsv
rm "${CLICKHOUSE_TMP}"/data2500.tsv
rm "${CLICKHOUSE_TMP}"/data2500_load_from_model.tsv
