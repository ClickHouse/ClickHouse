DROP TABLE IF EXISTS tab SYNC;

CREATE TABLE tab
(
    a DateTime,
    pk String
) Engine = MergeTree() ORDER BY pk;

INSERT INTO tab select cast(number, 'DateTime'), generateUUIDv4() FROM system.numbers LIMIT 1;

SELECT count(*) FROM tab WHERE a = '2024-08-06 09:58:09';
SELECT count(*) FROM tab WHERE a = '2024-08-06 09:58:0';  -- { serverError CANNOT_PARSE_DATETIME }
SELECT count(*) FROM tab WHERE a = '2024-08-0 09:58:09';  -- { serverError TYPE_MISMATCH }

DROP TABLE IF EXISTS tab SYNC;
