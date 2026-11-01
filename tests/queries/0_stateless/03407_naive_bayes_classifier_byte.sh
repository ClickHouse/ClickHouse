#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Output language code mapping:
#   Bengali           0
#   Mandarin Chinese  1
#   German            2
#   Greek             3
#   English           4
#   French            5
#   Russian           6
#   Spanish           7

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_byte_data"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_byte_2"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_byte_data (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_byte_data FROM INFILE '$CURDIR/data_naive_bayes_classifier/nb_model_lang_byte_2.tsv.zst' FORMAT TabSeparated"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY lang_byte_2
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_byte_data'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' alpha 1.0 priors_mode 'proportional' start_token '0x01' end_token '0xFF'))
LIFETIME(0)
"

$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'বইটি টেবিলের উপর রাখা আছে।')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', '他们正在公园里散步')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'Er kocht Suppe für seine Familie')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'Η βροχή σταμάτησε πριν από λίγο')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'She painted the wall a bright yellow')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'Nous attendons le bus depuis dix minutes.')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'На кухне пахнет свежим хлебом.')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_byte_2', 'Los niños juegan en la arena.')"

$CLICKHOUSE_CLIENT -q "SELECT dictGet('lang_byte_2', 'class_id', 'She painted the wall a bright yellow')"
$CLICKHOUSE_CLIENT -q "SELECT dictGet('lang_byte_2', 'class_id', 'На кухне пахнет свежим хлебом.')"

$CLICKHOUSE_CLIENT -q "SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('lang_byte_2', 'She painted the wall a bright yellow') AS w)"
$CLICKHOUSE_CLIENT -q "SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('lang_byte_2', 'На кухне пахнет свежим хлебом.') AS w)"

$CLICKHOUSE_CLIENT -q "SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('lang_byte_2', 'She painted the wall a bright yellow'))"
$CLICKHOUSE_CLIENT -q "SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('lang_byte_2', 'На кухне пахнет свежим хлебом.'))"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_byte_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_byte_data"
