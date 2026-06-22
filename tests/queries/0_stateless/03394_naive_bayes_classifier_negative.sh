#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_neg_data"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS nb_neg_model"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_neg_data (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_neg_data FROM INFILE '$CURDIR/data_naive_bayes_classifier/nb_model_lang_byte_2.tsv.zst' FORMAT TabSeparated"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY nb_neg_model
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_neg_data'))
LAYOUT(NAIVE_BAYES(n 2 mode 'byte' alpha 1.0))
LIFETIME(0)
"

$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('sentiment', 3)" 2>&1 | grep -om1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier(0, 'hello')" 2>&1 | grep -om1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('zzz_nonexistent_model_4ae239f8', 'hello')" 2>&1 | grep -om1 'BAD_ARGUMENTS'

# Empty input not allowed
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifier('nb_neg_model', '')" 2>&1 | grep -om1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifierWithProb('nb_neg_model', '')" 2>&1 | grep -om1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT naiveBayesClassifierAllProbs('nb_neg_model', '')" 2>&1 | grep -om1 'BAD_ARGUMENTS'
# dictGet with empty input is handled by the dictionary's getColumn — returns default class based on priors

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS nb_neg_model"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_neg_data"
