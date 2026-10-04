-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- A glob that is not a single `{a,b,c}` selection is matched by the reader as a regexp, and RE2
-- refuses to compile one that is too large - here a group of 100000 distinct alternatives (RE2 gives
-- up from about 50000 of them). The reader fails with `CANNOT_COMPILE_REGEXP` before any object is
-- listed, so resolving the sample path for hive partitioning must report the same refusal instead of
-- swallowing it as a listing failure that only leaves the `date` hive column unresolved.
-- Building and refusing such a regexp is slow under sanitizers, so there are as few of them as can be.

SET use_hive_partitioning = 1;

-- Next to a `*`, the sample path is listed.
SELECT date
FROM s3('http://localhost:11111/test/05237/date=2020-01-01/{'
            || arrayStringConcat(arrayMap(x -> hex(sipHash64(x)), range(100000)), ',') || '}*.tsv',
        'test', 'testtest', 'TSV', 'c1 UInt64'); -- { serverError CANNOT_COMPILE_REGEXP }

-- A table definition defers the resolution to the first use, and tolerating a listing failure there
-- does not tolerate a refused path either.
CREATE TABLE t_glob_uncompilable (c1 UInt64)
ENGINE = S3('http://localhost:11111/test/05237/date=2020-01-01/{'
                || arrayStringConcat(arrayMap(x -> hex(sipHash64(x)), range(100000)), ',') || '}*.tsv',
            'test', 'testtest', 'TSV');

DESC t_glob_uncompilable SETTINGS throw_on_hive_partitioning_resolution_failure = 0; -- { serverError CANNOT_COMPILE_REGEXP }

DROP TABLE t_glob_uncompilable;

-- Next to a second `{a,b,c}` group, the sample path would be the first alternative of each group,
-- but the reader matches that pattern as a regexp too, so the sample path is not taken from it.
CREATE TABLE t_glob_uncompilable_selector (c1 UInt64)
ENGINE = S3('http://localhost:11111/test/05237/date=2020-01-01/{'
                || arrayStringConcat(arrayMap(x -> hex(sipHash64(x)), range(100000)), ',') || '}{x,y}.tsv',
            'test', 'testtest', 'TSV');

DESC t_glob_uncompilable_selector SETTINGS throw_on_hive_partitioning_resolution_failure = 0; -- { serverError CANNOT_COMPILE_REGEXP }

DROP TABLE t_glob_uncompilable_selector;

-- A regexp that compiles still resolves the `date` hive column from the listed sample path.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05237/date=2020-01-01/a.tsv',
                        'test', 'testtest', 'TSV', 'c1 UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 1;

SELECT date, c1
FROM s3('http://localhost:11111/test/05237/date=2020-01-01/{a,b}*.tsv', 'test', 'testtest', 'TSV', 'c1 UInt64');
