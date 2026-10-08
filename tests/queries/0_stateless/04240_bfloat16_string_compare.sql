-- BFloat16 columns compared against string literals must match the stored (narrowed) value.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103682
-- Before the fix, `convertFieldToType` promoted BFloat16 to Float64 when parsing the string,
-- so '49.9' became Float64(49.9000000000000057) and the strict Float64->BFloat16 cast back
-- failed (49.9 != 49.75), yielding a Null Field and zero matches.

DROP TABLE IF EXISTS bf16_table;
CREATE TABLE bf16_table (my_field BFloat16) ENGINE=Memory;
INSERT INTO bf16_table VALUES (49.9), (0.1), (1.1), (3.14);

SELECT count() FROM bf16_table WHERE my_field = '49.9';
SELECT count() FROM bf16_table WHERE my_field = '0.1';
SELECT count() FROM bf16_table WHERE my_field = '1.1';
SELECT count() FROM bf16_table WHERE my_field = '3.14';
SELECT count() FROM bf16_table WHERE my_field IN ('49.9', '0.1');
SELECT count() FROM bf16_table WHERE my_field != '49.9';

DROP TABLE bf16_table;
