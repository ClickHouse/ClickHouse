-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

desc file('data_minio/{a,b,c}.tsv');
desc file('data_minio/{a,b,c}.tsv', 'TSV');
desc file('data_minio/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64');
desc file('data_minio/{a,b,c}.tsv');
desc file('data_minio/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto');
desc file('data_minio/{a,b,c}.tsv', 'TSV');
desc file('data_minio/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64');
desc file('data_minio/{a,b,c}.tsv', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto');


SELECT * FROM s3(decodeURLComponent(NULL), [NULL]);  --{serverError 170}
