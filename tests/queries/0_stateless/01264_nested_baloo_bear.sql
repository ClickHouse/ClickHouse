DROP TABLE IF EXISTS LOG_T;

CREATE TABLE LOG_T
(
    `fingerprint` UInt64, 
    `fields` Nested(
    name LowCardinality(String), 
    value String)
)
ENGINE = MergeTree
ORDER BY fingerprint;

SELECT
    fields.name,
    fields.value
FROM
(
    SELECT
        fields.name,
        fields.value
    FROM LOG_T
)
WHERE has(['node'], fields.value[indexOf(fields.name, 'ProcessName')]);

INSERT INTO LOG_T VALUES (123, ['Hello', 'ProcessName'], ['World', 'node']);

SELECT
    fields.name,
    fields.value
FROM
(
    SELECT
        fields.name,
        fields.value
    FROM LOG_T
)
WHERE has(['node'], fields.value[indexOf(fields.name, 'ProcessName')]);

DROP TABLE LOG_T;
