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

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_codepoint_data"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_codepoint_1"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_codepoint_data (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_codepoint_data FROM INFILE '$CURDIR/data_naive_bayes_classifier/nb_model_lang_codepoint_1.tsv.zst' FORMAT TabSeparated"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY lang_codepoint_1
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_codepoint_data'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint' alpha 1.0 priors_mode 'proportional'))
LIFETIME(0)
"

$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'আজ আকাশটা খুব পরিষ্কার।')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', '她每天早上喝一杯绿茶')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'Der Hund schläft unter dem Tisch.')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'Το ποδήλατο είναι δίπλα στο δέντρο.')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'He forgot his umbrella at the cafe.')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'Le chat regarde par la fenêtre')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'Мы гуляли в парке до заката')"
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('lang_codepoint_1', 'Ella escribe una carta a su abuela')"

$CLICKHOUSE_CLIENT -q "SELECT dictGet('lang_codepoint_1', 'class_id', 'He forgot his umbrella at the cafe.')"
$CLICKHOUSE_CLIENT -q "SELECT dictGet('lang_codepoint_1', 'class_id', 'Мы гуляли в парке до заката')"

$CLICKHOUSE_CLIENT -q "SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('lang_codepoint_1', 'He forgot his umbrella at the cafe.') AS w)"
$CLICKHOUSE_CLIENT -q "SELECT (w.1, round(w.2, 4)) FROM (SELECT naiveBayesClassifierWithProb('lang_codepoint_1', 'Мы гуляли в парке до заката') AS w)"

$CLICKHOUSE_CLIENT -q "SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('lang_codepoint_1', 'He forgot his umbrella at the cafe.'))"
$CLICKHOUSE_CLIENT -q "SELECT arrayMap(p -> (p.1, round(p.2, 4)), naiveBayesClassifierWithAllProbs('lang_codepoint_1', 'Мы гуляли в парке до заката'))"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_codepoint_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_codepoint_data"
