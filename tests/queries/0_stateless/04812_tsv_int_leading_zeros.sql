-- The `Escaped` and `Raw` escaping rules read integers with `readIntTextUnsafe`, which used to treat the
-- first '0' as the whole value and reject a leading '+'. `CSV` and `JSON` have their own overrides in
-- `SerializationNumber` and route to the tolerant `readIntText`, so every `CSV` row below is the
-- expected-value oracle for the `TSV` row beside it; `Quoted` has no override and reaches the same unsafe
-- reader as `Escaped` and `Raw`, which group 13 witnesses. `TSKV` and `CustomSeparated` need a trailing
-- newline for any value at all, including a plain 42, so the fields here carry one.

-- 1. Unsigned zero-padded integers with an explicit structure, in all four formats of the family.
SELECT 'group 1: TSV';
SELECT * FROM format(TSV, 'a Int64', '007\n00\n03242\n0100\n09\n01\n00000\n');
SELECT 'group 1: TSKV';
SELECT * FROM format(TSKV, 'a Int64', 'a=007\na=00\na=03242\na=0100\na=09\na=01\na=00000\n');
SELECT 'group 1: TSVRaw';
SELECT * FROM format(TSVRaw, 'a Int64', '007\n00\n03242\n0100\n09\n01\n00000\n');
SELECT 'group 1: CustomSeparated';
SELECT * FROM format(CustomSeparated, 'a Int64', '007\n00\n03242\n0100\n09\n01\n00000\n');

-- 2. Signed zero-padded integers. This is the set inference already types as `Int64`, so before the fix
-- the server chose a type and then refused to read the file at it.
SELECT 'group 2: signed padded';
SELECT * FROM format(TSV, 'a Int64', '-007\n-00\n-03242\n-0100\n');

-- 3. A redundant leading plus, which `readIntTextUnsafe` used to reject for any integer, padded or not.
SELECT 'group 3: plus sign';
SELECT * FROM format(TSV, 'a Int64', '+7\n+007\n+0\n');

-- 4. A lone '-' still reads as zero, because `TabSeparatedRowInputFormat.cpp:500` documents exactly
-- that: "for signed types, a string consisting of just a minus sign as a zero". These rows are
-- regression guards for that promise, not witnesses -- they read identically before and after. `CSV`
-- never made the promise and rejects the same input, so the two formats legitimately disagree here.
SELECT 'group 4: a lone minus is documented to read as zero';
SELECT * FROM format(TSV, 'a Int64', '-\n');
SELECT * FROM format(TSKV, 'a Int64', 'a=-\n');
SELECT * FROM format(CustomSeparated, 'a Int64', '-\n');
SELECT * FROM format(TSV, 'a Int64, b Int64', '-\t1\n');
-- `TSVRaw` keeps no trailing delimiter for the reader, so the '-' hits true end of stream instead.
SELECT * FROM format(TSVRaw, 'a Int64', '-\n'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
-- A lone '+' is rejected by the reader, and that IS this change: a '+' is now consumed as a sign, so
-- without a digit requirement on that branch the field would read as the value 0. Master left the '+'
-- unconsumed and the format layer refused the remainder, so end to end this moves 27 -> 72.
SELECT 'group 4: a sign followed by no digit -- the plus is rejected by the reader';
SELECT * FROM format(TSV, 'a Int64', '+\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(TSV, 'a Int64', '+x\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(TSV, 'a Int64', '+-\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(TSV, 'a Int64', '++\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(TSV, 'a Int64, b Int64', '+\t1\n'); -- { serverError CANNOT_PARSE_NUMBER }
-- The '-'-first forms are rejected by the FORMAT layer instead: the reader stops at the byte after the
-- sign and leaves it unconsumed, and `TSV` then refuses the remainder. Different layer, same rejection,
-- and identical to master -- these rows do not depend on this change at all.
SELECT 'group 4: a minus followed by no digit -- rejected by the format layer';
SELECT * FROM format(TSV, 'a Int64', '-x\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a Int64', '--\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a Int64', '-+\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a Int64', '- \n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT 'group 4: two signs are rejected even when a digit follows';
SELECT * FROM format(TSV, 'a Int64', '+-7\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(TSV, 'a Int64', '++7\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(TSV, 'a Int64', '-+7\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a Int64', '--7\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a Int64', '-+0\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a Int64', '-+007\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT 'group 4: CSV rejects every one of them too';
SELECT * FROM format(CSV, 'a Int64', '-\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(CSV, 'a Int64', '+\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(CSV, 'a Int64', '--\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(CSV, 'a Int64, b Int64', '-,1\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(CSV, 'a Int64', '-+7\n'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(CSV, 'a Int64', '++7\n'); -- { serverError CANNOT_PARSE_NUMBER }

-- 5. The load-bearing carrier, with no explicit structure: inference typed these `Int64` both before and
-- after, so pairing DESC with the read is what asserts that inference and the reader now agree rather
-- than merely that a read works. A sign before the zeros is the spelling group 6's guard does not reach,
-- so these keep inferring a number and the padding is not kept, exactly as `CSV` does for them.
SELECT 'group 5: inference and the reader agree';
DESC format(TSV, '-007');
SELECT * FROM format(TSV, '-007');
DESC format(TSV, '+007');
SELECT * FROM format(TSV, '+007');
DESC format(TSV, '+7');
SELECT * FROM format(TSV, '+7');

-- 6. Schema inference is deliberately not changed: a leading zero is significant in these formats, so a
-- field that starts with one and infers an integer keeps inferring `String`, even though the reader can
-- now read it as a number. The guard tests the first character and the inferred type, so it reaches
-- neither a sign (group 5) nor a zero-leading float (group 10). Pinned here so a future reader change
-- cannot move it silently.
SELECT 'group 6: padded integers keep inferring String';
DESC format(TSV, '007');
SELECT * FROM format(TSV, '007');
DESC format(TSV, '018446744073709551615');
SELECT * FROM format(TSV, '018446744073709551615');

-- 7. The oracle. These rows were already correct and must stay byte-identical.
SELECT 'group 7: CSV oracle';
SELECT * FROM format(CSV, 'a Int64', '007\n-007\n+7\n');
SELECT 'group 7: JSONEachRow control';
SELECT * FROM format(JSONEachRow, 'a Int64', '{"a":7}\n{"a":-7}\n');

-- 8. Unsigned target types. A plus is accepted because the plus branch is not guarded on signedness,
-- while a minus stays rejected because that branch is. The third row is what stops the fix from
-- silently making a negative value readable as unsigned.
SELECT 'group 8: UInt64 target';
SELECT * FROM format(TSV, 'a UInt64', '007\n');
SELECT * FROM format(TSV, 'a UInt64', '+7\n');
SELECT * FROM format(TSV, 'a UInt64', '-7\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a UInt64', '-007\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(TSV, 'a UInt32', '007\n');
SELECT * FROM format(TSV, 'a UInt32', '-7\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT 'group 8: wide integer targets take the same reader';
SELECT * FROM format(TSV, 'a Int128', '007\n-007\n+7\n');
SELECT * FROM format(TSV, 'a Int256', '007\n-007\n+7\n');
SELECT * FROM format(TSV, 'a UInt128', '007\n+7\n');
SELECT * FROM format(TSV, 'a UInt128', '-7\n'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- 9. Overflow is unchanged: leading zeros do not alter the accumulated value, and this reader has never
-- checked for overflow. Pinned so a future check cannot land here silently.
SELECT 'group 9: overflow parity with CSV';
SELECT * FROM format(TSV, 'a Int64', '018446744073709551615\n');
SELECT * FROM format(TSV, 'a Int64', '18446744073709551615\n');
SELECT * FROM format(CSV, 'a Int64', '018446744073709551615\n');
SELECT * FROM format(CSV, 'a Int64', '18446744073709551615\n');

-- 10. Floats are reached through a different branch of the same function and must not move at all.
SELECT 'group 10: floats unchanged';
DESC format(TSV, '0.5');
DESC format(TSV, '-0.5');
DESC format(TSV, '00.5');
DESC format(TSV, '-00.5');
DESC format(TSV, '+00.5');
DESC format(TSV, '.5');
DESC format(TSV, '-.5');
DESC format(TSV, '-.0');
SELECT 'group 10: and they read back as before';
SELECT * FROM format(TSV, '0.5');
SELECT * FROM format(TSV, '-0.5');
SELECT * FROM format(TSV, '00.5');
SELECT * FROM format(TSV, '-00.5');
SELECT * FROM format(TSV, '+00.5');
SELECT * FROM format(TSV, '.5');
SELECT * FROM format(TSV, '-.5');
SELECT * FROM format(TSV, '-.0');
SELECT * FROM format(TSV, 'x Float64', '007\n-007\n+7\n00\n');

-- 11. Edges of the run of zeros. The last row puts a zero in a column followed by a delimiter rather
-- than end of stream, so a zero at both a delimiter and end of stream is covered.
SELECT 'group 11: zero run edges';
SELECT * FROM format(TSV, 'a Int64', '0\n-0\n+0\n000000000000000000007\n');
SELECT * FROM format(TSV, 'a Int64, b Int64', '9\t0\n0\t9\n');

-- 12. A whole file, not a single field: a signed padded first row is what inference types `Int64`, so
-- before the fix reading the file failed after the type was already chosen.
SELECT 'group 12: whole file reads';
SELECT count() FROM format(TSV, '-007\n123\n456');
SELECT count() FROM format(TSV, '+1\n123\n456');

-- 13. The carriers that reach this reader outside the four formats above, one witness each.
-- `MySQLDump` reads values with the `Quoted` rule; a query parameter other than `_request_body` uses
-- the `Escaped` rule; a dictionary `null_value` is read with `deserializeWholeText`, which
-- `SimpleTextSerialization` routes into the same `deserializeText`.
SELECT 'group 13: MySQLDump, the Quoted rule';
SELECT * FROM format(MySQLDump, 'a Int64', 'INSERT INTO t VALUES (007),(-007),(+7);');
SELECT 'group 13: a query parameter';
SET param_pad = '007';
SELECT {pad:Int64};
SET param_plus = '+7';
SELECT {plus:Int64};
SELECT 'group 13: a dictionary null_value';
CREATE TABLE src_04812 (id UInt64, v Int64) ENGINE = Memory;
INSERT INTO src_04812 VALUES (1, 42);
CREATE DICTIONARY dict_04812 (id UInt64, v Int64 DEFAULT '007')
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_04812')) LAYOUT(FLAT()) LIFETIME(0);
SELECT dictGet('dict_04812', 'v', toUInt64(999));
DROP DICTIONARY dict_04812;
DROP TABLE src_04812;
-- `Values` reaches the reader through the `Quoted` rule, but a value its streaming parser rejects is
-- re-read by the SQL expression parser, so at default settings these read the same before and after.
SELECT 'group 13: Values, unchanged at default settings';
SELECT * FROM format(Values, 'a Int64', '(007),(+007),(-007)');
-- The one spelling that does move. With template deduction off, a padded or `+`-signed value that
-- overflows the target used to reach that fallback, which defaulted it to 0 (or raised TYPE_MISMATCH
-- under `input_format_null_as_default = 0`); the reader now consumes it and wraps, which is what the
-- unpadded spelling in the third row has always done. This reader has never checked overflow (group 9).
SELECT 'group 13: Values, an overflowing padded value now wraps like the unpadded one';
SELECT * FROM format(Values, 'a Int8', '(0128)') SETTINGS input_format_values_deduce_templates_of_expressions = 0;
SELECT * FROM format(Values, 'a Int8', '(+128)') SETTINGS input_format_values_deduce_templates_of_expressions = 0;
SELECT * FROM format(Values, 'a Int8', '(128)') SETTINGS input_format_values_deduce_templates_of_expressions = 0;

-- 14. The reader also serves SQL, not only a format: comparing a numeric column with a string literal
-- sends the literal through `convertFieldToType`, which re-reads it with `deserializeWholeText`. `toInt64`
-- has always used the tolerant reader for the same spellings, so it is the oracle here the way `CSV` is
-- above, and the comparison disagreed with it until now.
SELECT 'group 14: a string literal compared with a numeric column';
CREATE TABLE cmp_04812 (id UInt64, v Int64, INDEX bf_04812 v TYPE bloom_filter) ENGINE = MergeTree ORDER BY id
    AS SELECT number AS id, number AS v FROM numbers(8);
SELECT v FROM cmp_04812 WHERE v = '007';
SELECT v FROM cmp_04812 WHERE v = '+7';
SELECT toInt64('007'), toInt64('+7');
-- A literal that is not a number at all is still refused, by the same layer as before.
SELECT count() FROM cmp_04812 WHERE v = '0abc'; -- { serverError TYPE_MISMATCH }
-- The same conversion runs during planning, so the predicate now reaches the primary key and a skip index
-- instead of failing analysis. Only the naming lines are asserted: the part and granule counts beside them
-- move with the granularity the runner randomizes.
SELECT 'group 14: index analysis accepts the same literal';
SELECT trimBoth(explain) FROM (EXPLAIN indexes = 1 SELECT v FROM cmp_04812 WHERE id = '007') WHERE explain ILIKE '%Condition:%';
SELECT trimBoth(explain) FROM (EXPLAIN indexes = 1 SELECT v FROM cmp_04812 WHERE v = '007') WHERE explain ILIKE '%Name:%';
DROP TABLE cmp_04812;
