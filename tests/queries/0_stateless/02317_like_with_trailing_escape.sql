DROP TABLE IF EXISTS tab;

CREATE TABLE tab (haystack String, pattern String) engine = MergeTree() ORDER BY haystack;

INSERT INTO tab VALUES ('haystack', 'pattern\\');

-- const pattern
SELECT haystack LIKE 'pattern\\' from tab; -- { serverError 25 }

-- non-const pattern
SELECT haystack LIKE pattern from tab; -- { serverError 25 }

DROP TABLE IF EXISTS tab;
