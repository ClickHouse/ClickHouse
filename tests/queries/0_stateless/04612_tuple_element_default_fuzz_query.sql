-- Regression test: `fuzzQuery` must accept valid DDL statements that use `DEFAULT` expressions
-- inside `Tuple` data types. The fuzzer used to construct a real data type from the raw column
-- type AST before the tuple element defaults were normalized away, which threw `BAD_ARGUMENTS`.

SELECT * FROM fuzzQuery('CREATE TABLE t (c Tuple(a UInt8 DEFAULT 1, s String DEFAULT \'Hello\')) ENGINE = Memory', 500, 8956) LIMIT 10 FORMAT Null;
SELECT * FROM fuzzQuery('ALTER TABLE t ADD COLUMN c Tuple(a UInt8, s String DEFAULT \'Hello\')', 500, 5) LIMIT 10 FORMAT Null;
SELECT * FROM fuzzQuery('CREATE TABLE t (c Tuple(a UInt8, t Tuple(b String DEFAULT \'x\', c UInt8))) ENGINE = Memory', 500, 8956) LIMIT 10 FORMAT Null;

SELECT 'OK';
