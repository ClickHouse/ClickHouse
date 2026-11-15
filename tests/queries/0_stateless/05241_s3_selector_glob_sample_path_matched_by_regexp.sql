-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- A pattern with several `{a,b,c}` groups is matched by the reader as a regexp, so the sample path
-- `getPathSample` takes from the first alternative of every group is a sample of what is read only
-- when that regexp matches it. An empty alternative is literal text for the regexp: the reader looks
-- for the key `{,x}a.tsv` with the braces in it, not for `a.tsv` and `xa.tsv`. So the sample path
-- is listed the same way as the reader lists it, and with nothing listed the `date` hive column is
-- not resolved from a path the reader never reads.

SET use_hive_partitioning = 1;

SELECT date
FROM s3('http://localhost:11111/test/05241/date={2020-01-01,2020-01-02}/{,x}{a,b}.tsv',
        'test', 'testtest', 'TSV', 'c1 UInt64')
SETTINGS s3_throw_on_zero_files_match = 0; -- { serverError UNKNOWN_IDENTIFIER }

SELECT date
FROM s3Cluster('test_shard_localhost',
               'http://localhost:11111/test/05241/date={2020-01-01,2020-01-02}/{,x}{a,b}.tsv',
               'test', 'testtest', 'TSV', 'c1 UInt64')
SETTINGS s3_throw_on_zero_files_match = 0; -- { serverError UNKNOWN_IDENTIFIER }

-- Groups the regexp matches the same way still give the sample path without listing anything.
SELECT count()
FROM (SELECT date
      FROM s3('http://localhost:11111/test/05241/date={2020-01-01,2020-01-02}/{x,y}{a,b}.tsv',
              'test', 'testtest', 'TSV', 'c1 UInt64')
      SETTINGS s3_throw_on_zero_files_match = 0);
