#!/usr/bin/env bash
# Tags: long

# Accuracy regression test for the codepoint-bigram language-detection model.
#
# The model distinguishes 8 languages (ben, cmn, deu, ell, eng, fra, rus, spa). On the full
# held-out test set it scores ~99.4%; on this committed 300-sentences-per-language subsample it
# scores ~99.6% overall, with the weakest class (English) at ~0.97. We assert robust thresholds so
# the test guards against regressions in the classifier or tokenization without being brittle.
#
# The model was trained with U+10FFFE / U+10FFFF boundary code points, so the dictionary specifies matching
# start_token / end_token to use those features (padding is opt-in and off by default). This also exercises
# the padding code path on real data.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_naive_bayes_classifier"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_codepoint_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_lang_model"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_lang_eval"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_lang_model (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_lang_model FROM INFILE '$DATA/nb_model_lang_codepoint_2.tsv.zst' FORMAT TabSeparated"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY lang_codepoint_2
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_lang_model'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0x10FFFE' end_token '0x10FFFF'))
LIFETIME(0)
"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_lang_eval (class_id UInt32, text String) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_lang_eval FROM INFILE '$DATA/nb_lang_eval.tsv.zst' FORMAT TabSeparated"

# Overall accuracy (~0.996) stays above a robust floor.
$CLICKHOUSE_CLIENT -q "SELECT avg(naiveBayesClassifier('lang_codepoint_2', text) = class_id) >= 0.98 FROM nb_lang_eval"

# No single language collapses (weakest class is ~0.97).
$CLICKHOUSE_CLIENT -q "
SELECT min(class_accuracy) >= 0.95
FROM
(
    SELECT class_id, avg(naiveBayesClassifier('lang_codepoint_2', text) = class_id) AS class_accuracy
    FROM nb_lang_eval
    GROUP BY class_id
)
"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_codepoint_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_lang_model"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_lang_eval"
