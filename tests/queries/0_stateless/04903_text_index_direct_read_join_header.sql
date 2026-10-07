-- Random settings limits: query_plan_direct_read_from_text_index=(1, 1); use_skip_indexes=(1, 1); query_plan_remove_unused_columns=(1, 1)

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_04903;

CREATE TABLE tab_04903
(
    id UInt32,
    key String,
    INDEX idx_key (key) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_04903 VALUES (1, 'foo');

SELECT t.id
FROM tab_04903 AS t
GLOBAL JOIN (SELECT toUInt32(number) AS rid FROM numbers(2)) AS r ON t.id = r.rid
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo');

DROP TABLE tab_04903;
