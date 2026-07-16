DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    c0 Tuple(c1 String, c2 String),
    INDEX i0 c0.c1 TYPE text(tokenizer = splitByString)
)
ENGINE = SummingMergeTree() ORDER BY (id);

INSERT INTO TABLE tab (id, c0) VALUES (1, ('a aa aaa', 'b bb bbb'));
INSERT INTO TABLE tab (id, c0) VALUES (1, ('c cc ccc', 'd dd ddd'));

OPTIMIZE TABLE tab FINAL;

SELECT id FROM tab WHERE hasAllTokens(c0.c1, 'aa aaa') SETTINGS force_data_skipping_indices = 'i0';

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt64,
    c1 JSON(s1 String),
    INDEX i0 c1.s1 TYPE text(tokenizer = splitByString),
    INDEX i1 coalesce(c1.s2, '')::String TYPE text(tokenizer = splitByString),
)
ENGINE = SummingMergeTree() ORDER BY (id);

INSERT INTO TABLE tab (id, c1) VALUES (1, '{"s1": "AAA"}');
INSERT INTO TABLE tab (id, c1) VALUES (2, '{"s2": "BBB"}');

OPTIMIZE TABLE tab FINAL;

SELECT id FROM tab WHERE hasAllTokens(c1.s1, 'AAA') SETTINGS force_data_skipping_indices = 'i0';
SELECT id FROM tab WHERE hasAllTokens(coalesce(c1.s2, '')::String, 'BBB') SETTINGS force_data_skipping_indices = 'i1';

DROP TABLE tab;
