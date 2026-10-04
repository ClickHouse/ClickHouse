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

# Setup: load both language models
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_byte_data_gen"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_codepoint_data_gen"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_byte_2"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_codepoint_1"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_byte_data_gen (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_byte_data_gen FROM INFILE '$CURDIR/data_naive_bayes_classifier/nb_model_lang_byte_2.tsv.zst' FORMAT TabSeparated"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY lang_byte_2
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_byte_data_gen'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 2 mode 'byte' alpha 1.0 priors_mode 'proportional' start_token '0x01' end_token '0xFF'))
LIFETIME(0)
"

$CLICKHOUSE_CLIENT -q "CREATE TABLE nb_codepoint_data_gen (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram)"
$CLICKHOUSE_CLIENT -q "INSERT INTO nb_codepoint_data_gen FROM INFILE '$CURDIR/data_naive_bayes_classifier/nb_model_lang_codepoint_1.tsv.zst' FORMAT TabSeparated"

$CLICKHOUSE_CLIENT -q "
CREATE DICTIONARY lang_codepoint_1
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_codepoint_data_gen'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'codepoint' alpha 1.0 priors_mode 'proportional'))
LIFETIME(0)
"

# Batch with numbers().
# The dictionary name is qualified with the database so it resolves on remote replicas too: with
# parallel replicas the secondary queries carry a fully-qualified table but keep their own current
# database, so an unqualified dictionary name would not resolve there (same as `dictGet`).
$CLICKHOUSE_CLIENT -q "SELECT number, naiveBayesClassifier('${CLICKHOUSE_DATABASE}.lang_byte_2', 'She painted the wall a bright yellow') FROM numbers(10) ORDER BY number"

# Multiple models x multiple inputs. The model name must be a constant, so each model is a
# separate UNION ALL branch with a literal name.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS input_texts"
$CLICKHOUSE_CLIENT -q "CREATE TABLE input_texts (input_text String) ENGINE = MergeTree() ORDER BY input_text"
$CLICKHOUSE_CLIENT -q "INSERT INTO input_texts VALUES
('He fixed the broken chair yesterday'),
('The sun came out after the storm'),
('Sie liest jeden Abend ein spannendes Buch.'),
('Ο σκύλος κοιμάται δίπλα στο τζάκι.'),
('El gato observa a los pájaros desde la ventana.'),
('В саду распустились красные тюльпаны.'),
('Nous préparons le dîner pour nos invités.'),
('They have finished their homework already'),
('孩子们在花园里追逐蝴蝶。'),
('সে প্রতিদিন ভোরে দৌড়াতে যায়।')"

$CLICKHOUSE_CLIENT -q "
SELECT model_name, input_text, classification
FROM
(
    SELECT 'lang_byte_2' AS model_name, input_text, naiveBayesClassifier('${CLICKHOUSE_DATABASE}.lang_byte_2', input_text) AS classification FROM input_texts
    UNION ALL
    SELECT 'lang_codepoint_1' AS model_name, input_text, naiveBayesClassifier('${CLICKHOUSE_DATABASE}.lang_codepoint_1', input_text) AS classification FROM input_texts
)
ORDER BY model_name, input_text
"

$CLICKHOUSE_CLIENT -q "
SELECT model_name, input_text, (result.1, round(result.2, 4))
FROM
(
    SELECT 'lang_byte_2' AS model_name, input_text, naiveBayesClassifierWithProb('${CLICKHOUSE_DATABASE}.lang_byte_2', input_text) AS result FROM input_texts
    UNION ALL
    SELECT 'lang_codepoint_1' AS model_name, input_text, naiveBayesClassifierWithProb('${CLICKHOUSE_DATABASE}.lang_codepoint_1', input_text) AS result FROM input_texts
)
ORDER BY model_name, input_text
"

$CLICKHOUSE_CLIENT -q "
SELECT model_name, input_text, arrayMap(p -> (p.1, round(p.2, 4)), result)
FROM
(
    SELECT 'lang_byte_2' AS model_name, input_text, naiveBayesClassifierWithAllProbs('${CLICKHOUSE_DATABASE}.lang_byte_2', input_text) AS result FROM input_texts
    UNION ALL
    SELECT 'lang_codepoint_1' AS model_name, input_text, naiveBayesClassifierWithAllProbs('${CLICKHOUSE_DATABASE}.lang_codepoint_1', input_text) AS result FROM input_texts
)
ORDER BY model_name, input_text
"

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS input_texts"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_byte_2"
$CLICKHOUSE_CLIENT -q "DROP DICTIONARY IF EXISTS lang_codepoint_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_byte_data_gen"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS nb_codepoint_data_gen"
