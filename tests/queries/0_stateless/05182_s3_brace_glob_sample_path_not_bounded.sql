-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- `getPathSample` needs one path to infer hive partition columns from, and used to get it by
-- expanding the whole `{a,b,c}` Cartesian product. The reader enumerates only a pattern with
-- exactly one brace group and matches anything with more as a regexp, so bounding the sample path
-- by the product failed a pattern the reader handles. Below asks for 2^17 combinations.
-- The reader lists everything under the prefix before the first group, so the path has a prefix of
-- its own: the bucket root holds the data of the server's own disks with the s3 storage.

SET use_hive_partitioning = 1;

SELECT count()
FROM s3('http://localhost:11111/test/05182/{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}.tsv',
        'test', 'testtest', 'TSV', 'c1 UInt64');
