-- Row analogue of 04669_join_key_no_supertype: JOIN keys of Row types whose fields have
-- no least supertype, such as Row(x UInt64) and Row(x Int64), fall back to the common subtype.

SET allow_experimental_row_type = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS row_join_unsigned;
DROP TABLE IF EXISTS row_join_signed;

CREATE TABLE row_join_unsigned (r Row(x UInt64)) ENGINE = Memory;
CREATE TABLE row_join_signed (r Row(x Int64)) ENGINE = Memory;

INSERT INTO row_join_unsigned VALUES ((0)), ((1)), ((9223372036854775807)), ((9223372036854775808)), ((18446744073709551615));
INSERT INTO row_join_signed VALUES ((-9223372036854775808)), ((-1)), ((0)), ((1)), ((9223372036854775807));

SELECT 'INNER';
SELECT a.r, b.r FROM row_join_unsigned AS a INNER JOIN row_join_signed AS b ON a.r = b.r ORDER BY ALL;

SELECT 'SEMI';
SELECT a.r FROM row_join_unsigned AS a SEMI LEFT JOIN row_join_signed AS b ON a.r = b.r ORDER BY ALL;

SELECT 'The types of the result are not affected';
SELECT toTypeName(a.r), toTypeName(b.r) FROM row_join_unsigned AS a INNER JOIN row_join_signed AS b ON a.r = b.r LIMIT 1;

SELECT 'A Row key against a Tuple key';
SELECT a.r, b.t FROM row_join_unsigned AS a INNER JOIN (SELECT tuple(CAST(1, 'Int64')) AS t) AS b ON a.r = b.t ORDER BY ALL;

SELECT 'A Row with a field that accurateCastOrNull cannot represent is rejected';
SELECT * FROM (SELECT CAST(tuple([1]), 'Row(x Array(UInt64))') AS r) AS a
INNER JOIN (SELECT CAST(tuple([1]), 'Row(x Array(Int64))') AS r) AS b ON a.r = b.r; -- { serverError NO_COMMON_TYPE }

DROP TABLE row_join_unsigned;
DROP TABLE row_join_signed;
