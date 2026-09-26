SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

CREATE TABLE json_path_values_null_pattern
(
    data JSON(url String),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO json_path_values_null_pattern VALUES ('{"url":"example"}');

SELECT count()
FROM json_path_values_null_pattern
WHERE match(data.url, CAST(NULL, 'Nullable(String)'));

DROP TABLE json_path_values_null_pattern;
