DROP TABLE IF EXISTS normalize_test;
CREATE TABLE normalize_test (value String) ENGINE = MergeTree ORDER BY value;

SELECT
    'ё' AS norm,
    'ё' AS denorm,
    length(norm),
    length(denorm),
    normalizeUTF8(norm),
    normalizeUTF8(denorm),
    length(normalizeUTF8(norm)),
    length(normalizeUTF8(denorm));

INSERT INTO normalize_test (value) VALUES ('ё');
INSERT INTO normalize_test (value) VALUES ('ё');

SELECT value, length(value), normalizeUTF8(value) AS normalized, length(normalized) FROM normalize_test;

SELECT char(228) AS value, normalizeUTF8(value); -- { serverError 619 }
