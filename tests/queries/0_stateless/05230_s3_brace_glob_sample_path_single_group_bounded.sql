-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- `getPathSample` needs one path to infer hive partition columns from, and takes it from the first
-- alternative of every `{a,b,c}` group without enumerating the rest. That is right only for the
-- shapes the reader matches as a regexp: a pattern with exactly one group is materialized by the
-- reader through the bounded `expandSelectionGlob`, so a sample path for that shape has to obey the
-- same limits. Otherwise analysis infers hive partition columns - and a table definition keeps them
-- - for a path the reader always refuses to enumerate.

SET use_hive_partitioning = 1;

-- Several groups: the reader matches them as a regexp whatever they multiply out to, so the sample
-- path is taken from them and the `date` hive column is there.
SELECT count()
FROM (SELECT date FROM s3('http://localhost:11111/test/date=2020-01-01/{a,b}{a,b}{a,b}.tsv',
                          'test', 'testtest', 'TSV', 'c1 UInt64'));

-- One group asking for more paths than the reader will ever enumerate: the refusal is reported while
-- resolving the path, the same as it is when reading it - not swallowed as a listing failure that
-- would only leave the `date` hive column unresolved. Both the plain table function and its
-- `-Cluster` alternative report it the same way.
SELECT date
FROM s3('http://localhost:11111/test/date=2020-01-01/{' || repeat('a,', 1000000) || 'a}.tsv',
        'test', 'testtest', 'TSV', 'c1 UInt64'); -- { serverError BAD_ARGUMENTS }

SELECT date
FROM s3Cluster('test_shard_localhost',
               'http://localhost:11111/test/date=2020-01-01/{' || repeat('a,', 1000000) || 'a}.tsv',
               'test', 'testtest', 'TSV', 'c1 UInt64'); -- { serverError BAD_ARGUMENTS }

-- And a table definition does not keep hive partition columns for such a path either: resolving it
-- is refused exactly like reading it.
CREATE TABLE t_glob_huge (c1 UInt64)
ENGINE = S3('http://localhost:11111/test/date=2020-01-01/{' || repeat('a,', 1000000) || 'a}.tsv',
            'test', 'testtest', 'TSV');

DESC t_glob_huge; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_glob_huge; -- { serverError BAD_ARGUMENTS }

-- Tolerating a listing failure does not tolerate a refused path: it is not an endpoint problem that
-- the next query might not hit, so it is reported rather than retried by every query.
DESC t_glob_huge SETTINGS throw_on_hive_partitioning_resolution_failure = 0; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_glob_huge;

-- A regexp is more permissive than a selector glob: `makeRegexpPatternFromGlobs` reads a doubled
-- brace as a literal brace around an enum, and the reader matches such a path. The sample path for
-- it is listed instead of being parsed as a selector glob, so analysis accepts what the reader
-- reads - before, resolving the path threw `BAD_ARGUMENTS` on the second `{`.
SELECT count() FROM s3('http://localhost:11111/test/date=2020-01-01/{{a,b}}.tsv',
                       'test', 'testtest', 'TSV', 'c1 UInt64');

SELECT count() FROM s3Cluster('test_shard_localhost',
                              'http://localhost:11111/test/date=2020-01-01/{{a,b}}.tsv',
                              'test', 'testtest', 'TSV', 'c1 UInt64');

CREATE TABLE t_glob_doubled (c1 UInt64)
ENGINE = S3('http://localhost:11111/test/date=2020-01-01/{{a,b}}.tsv', 'test', 'testtest', 'TSV');

SELECT count() FROM t_glob_doubled;

DROP TABLE t_glob_doubled;

-- The same for a comma outside of any group, which is literal text for the regexp as well. Here the
-- object exists, so the sample path - and with it the `date` hive column - comes from the listing.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05230/date=2020-01-01/a,bce.tsv',
                        'test', 'testtest', 'TSV', 'c1 UInt64') SETTINGS s3_truncate_on_insert = 1 SELECT 1;

SELECT date, c1
FROM s3('http://localhost:11111/test/05230/date=2020-01-01/a,b{c,d}{e,f}.tsv',
        'test', 'testtest', 'TSV', 'c1 UInt64');
