CREATE TEMPORARY TABLE a (b UInt8) ENGINE = MergeTree ORDER BY b;
ALTER TABLE a MODIFY COLUMN b CODEC(`@`); -- { serverError UNKNOWN_CODEC }
SELECT f(`@`); -- { serverError UNKNOWN_IDENTIFIER }
SELECT `@`(1); -- { serverError UNKNOWN_FUNCTION }
SELECT ` `(1); -- { serverError UNKNOWN_FUNCTION }
SELECT `упячка`(1); -- { serverError UNKNOWN_FUNCTION }
