#!/usr/bin/env bash
# Tags: long

# End-to-end NAIVE_BAYES pipeline on raw labelled text (300 sentences x 8 languages, class ids 0..7):
#   1. build (ngram, class_id, count) training data straight from the text with naiveBayesNgrams + GROUP BY,
#   2. for all three tokenization modes (byte / codepoint / token), each with its recommended boundary padding,
#   3. with an explicit document-frequency prior computed here from the data,
#   4. then classify with each model.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_naive_bayes_classifier"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS nb_pipe_byte"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS nb_pipe_codepoint"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS nb_pipe_token"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_pipe_raw"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_pipe_byte_train"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_pipe_codepoint_train"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_pipe_token_train"

# Raw labelled text: (class_id, text).
$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_pipe_raw (class_id UInt32, text String) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_pipe_raw FROM INFILE '$DATA/nb_lang_eval.tsv.zst' FORMAT TabSeparated"
$CLICKHOUSE_CLIENT -q "SELECT 'loaded', count() = 2400 AND uniqExact(class_id) = 8 FROM nb_pipe_raw"

# Build (ngram, class_id, count) training data per mode with naiveBayesNgrams + GROUP BY, each with recommended padding.
$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_pipe_byte_train (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_pipe_byte_train SELECT ngram, class_id, count() FROM nb_pipe_raw ARRAY JOIN naiveBayesNgrams(text, 2, 'byte', '0x01', '0xFF') AS ngram GROUP BY ngram, class_id"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_pipe_codepoint_train (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_pipe_codepoint_train SELECT ngram, class_id, count() FROM nb_pipe_raw ARRAY JOIN naiveBayesNgrams(text, 2, 'codepoint', '0x10FFFE', '0x10FFFF') AS ngram GROUP BY ngram, class_id"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_pipe_token_train (ngram String, class_id UInt32, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_pipe_token_train SELECT ngram, class_id, count() FROM nb_pipe_raw ARRAY JOIN naiveBayesNgrams(text, 2, 'token', '<s>', '</s>') AS ngram GROUP BY ngram, class_id"

# Explicit document-frequency prior (documents in class / total documents), computed from the data and reused for all models.
PRIORS=$($CLICKHOUSE_CLIENT -q "
SELECT toString(groupArray((class_id, round(cnt / (SELECT count() FROM nb_pipe_raw), 9))))
FROM (SELECT class_id, count() AS cnt FROM nb_pipe_raw GROUP BY class_id)")

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY nb_pipe_byte (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pipe_byte_train'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' start_token '0x01' end_token '0xFF' priors_mode 'explicit' priors $PRIORS))
LIFETIME(0)"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY nb_pipe_codepoint (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pipe_codepoint_train'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'codepoint' start_token '0x10FFFE' end_token '0x10FFFF' priors_mode 'explicit' priors $PRIORS))
LIFETIME(0)"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY nb_pipe_token (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram SOURCE(CLICKHOUSE(TABLE 'nb_pipe_token_train'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'token' start_token '<s>' end_token '</s>' priors_mode 'explicit' priors $PRIORS))
LIFETIME(0)"

# Each model recovers its training labels well (a robust floor; the real numbers are >= 0.99).
$CLICKHOUSE_CLIENT -q "SELECT 'byte_recovers_labels', avg(naiveBayesClassifier('nb_pipe_byte', text) = class_id) >= 0.95 FROM nb_pipe_raw"
$CLICKHOUSE_CLIENT -q "SELECT 'codepoint_recovers_labels', avg(naiveBayesClassifier('nb_pipe_codepoint', text) = class_id) >= 0.95 FROM nb_pipe_raw"
$CLICKHOUSE_CLIENT -q "SELECT 'token_recovers_labels', avg(naiveBayesClassifier('nb_pipe_token', text) = class_id) >= 0.95 FROM nb_pipe_raw"

# Sample query: one sentence from each distinct-script language (Bengali, Chinese, Greek, Russian) classifies to its own language in every model.
$CLICKHOUSE_CLIENT -q "SELECT 'byte_distinct_scripts', countIf(naiveBayesClassifier('nb_pipe_byte', text) = class_id) = count() FROM (SELECT class_id, argMin(text, text) AS text FROM nb_pipe_raw WHERE class_id IN (0, 1, 3, 6) GROUP BY class_id)"
$CLICKHOUSE_CLIENT -q "SELECT 'codepoint_distinct_scripts', countIf(naiveBayesClassifier('nb_pipe_codepoint', text) = class_id) = count() FROM (SELECT class_id, argMin(text, text) AS text FROM nb_pipe_raw WHERE class_id IN (0, 1, 3, 6) GROUP BY class_id)"
$CLICKHOUSE_CLIENT -q "SELECT 'token_distinct_scripts', countIf(naiveBayesClassifier('nb_pipe_token', text) = class_id) = count() FROM (SELECT class_id, argMin(text, text) AS text FROM nb_pipe_raw WHERE class_id IN (0, 1, 3, 6) GROUP BY class_id)"

# The explicit prior is in effect: with no n-grams (empty input) the prediction is the prior's argmax (uniform here -> lowest class id).
$CLICKHOUSE_CLIENT -q "SELECT 'empty_input_uses_prior', naiveBayesClassifier('nb_pipe_codepoint', '') = 0"

# All three functions agree on a sentence, and the per-class probabilities sum to 1.
$CLICKHOUSE_CLIENT -q "
WITH (SELECT argMin(text, text) FROM nb_pipe_raw WHERE class_id = 6) AS s
SELECT 'functions',
       naiveBayesClassifier('nb_pipe_codepoint', s) = 6,
       naiveBayesClassifierWithProb('nb_pipe_codepoint', s).1 = 6,
       round(arraySum(arrayMap(p -> p.2, naiveBayesClassifierWithAllProbs('nb_pipe_codepoint', s))), 4) = 1
SETTINGS enable_analyzer = 1"

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY nb_pipe_byte"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY nb_pipe_codepoint"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY nb_pipe_token"
$CLICKHOUSE_CLIENT -q "DROP TABLE nb_pipe_raw"
$CLICKHOUSE_CLIENT -q "DROP TABLE nb_pipe_byte_train"
$CLICKHOUSE_CLIENT -q "DROP TABLE nb_pipe_codepoint_train"
$CLICKHOUSE_CLIENT -q "DROP TABLE nb_pipe_token_train"
