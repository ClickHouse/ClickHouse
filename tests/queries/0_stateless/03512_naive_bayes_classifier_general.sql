-- Tags: no-fasttest
-- no-fasttest: depends on model binary and model details via config files

/*
Output language code mapping:
  Bengali           0
  Mandarin Chinese  1
  German            2
  Greek             3
  English           4
  French            5
  Russian           6
  Spanish           7
*/

SELECT number, naiveBayesClassifier('lang_byte_2', 'She painted the wall a bright yellow')
FROM numbers(10) ORDER BY number;

DROP TABLE IF EXISTS model_names;
CREATE TABLE model_names (
    model_name String,
) ENGINE = MergeTree()
ORDER BY model_name;

INSERT INTO model_names VALUES
('lang_byte_2'),
('lang_codepoint_1');

DROP TABLE IF EXISTS input_texts;
CREATE TABLE input_texts (
    input_text String,
) ENGINE = MergeTree()
ORDER BY input_text;

INSERT INTO input_texts VALUES
('He fixed the broken chair yesterday'),
('The sun came out after the storm'),
('Sie liest jeden Abend ein spannendes Buch.'),
('Ο σκύλος κοιμάται δίπλα στο τζάκι.'),
('El gato observa a los pájaros desde la ventana.'),
('В саду распустились красные тюльпаны.'),
('Nous préparons le dîner pour nos invités.'),
('They have finished their homework already'),
('孩子们在花园里追逐蝴蝶。'),
('সে প্রতিদিন ভোরে দৌড়াতে যায়।');

SELECT
    model_name,
    input_text,
    naiveBayesClassifier(model_name, input_text) AS classification
FROM
    model_names
CROSS JOIN
    input_texts
ORDER BY
    model_name,
    input_text;
