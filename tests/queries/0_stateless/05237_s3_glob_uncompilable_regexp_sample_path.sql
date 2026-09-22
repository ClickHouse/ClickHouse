-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- A glob that is not a pure `{a,b,c}` selection is matched by the reader as a regexp, and RE2
-- refuses to compile one that is too large - here a single group of 200000 distinct alternatives
-- next to a `*`. The reader fails with `CANNOT_COMPILE_REGEXP` before any object is listed, so
-- resolving the sample path for hive partitioning must report the same refusal instead of
-- swallowing it as a listing failure that only leaves the `date` hive column unresolved.

SET use_hive_partitioning = 1;

SELECT date
FROM s3('http://localhost:11111/test/05237/date=2020-01-01/{'
            || arrayStringConcat(arrayMap(x -> hex(sipHash64(x)), range(200000)), ',') || '}*.tsv',
        'test', 'testtest', 'TSV', 'c1 UInt64'); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT date
FROM s3Cluster('test_shard_localhost',
               'http://localhost:11111/test/05237/date=2020-01-01/{'
                   || arrayStringConcat(arrayMap(x -> hex(sipHash64(x)), range(200000)), ',') || '}*.tsv',
               'test', 'testtest', 'TSV', 'c1 UInt64'); -- { serverError CANNOT_COMPILE_REGEXP }

-- A table definition defers the resolution to the first use, and tolerating a listing failure there
-- does not tolerate a refused path either.
CREATE TABLE t_glob_uncompilable (c1 UInt64)
ENGINE = S3('http://localhost:11111/test/05237/date=2020-01-01/{'
                || arrayStringConcat(arrayMap(x -> hex(sipHash64(x)), range(200000)), ',') || '}*.tsv',
            'test', 'testtest', 'TSV');

DESC t_glob_uncompilable SETTINGS throw_on_hive_partitioning_resolution_failure = 0; -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM t_glob_uncompilable; -- { serverError CANNOT_COMPILE_REGEXP }

DROP TABLE t_glob_uncompilable;

-- A regexp that compiles still resolves the `date` hive column from the listed sample path.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05237/date=2020-01-01/a.tsv',
                        'test', 'testtest', 'TSV', 'c1 UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 1;

SELECT date, c1
FROM s3('http://localhost:11111/test/05237/date=2020-01-01/{a,b}*.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64');
