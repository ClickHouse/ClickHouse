-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

select 's3, single archive';
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive2.zip :: example*.csv') ORDER BY (id, _file, _path);
select 's3Cluster: single archive';
SELECT id, data, _size, _file, _path FROM s3Cluster('test_cluster_two_shards', s3_conn, filename='03036_archive2.zip :: example*.csv') ORDER BY (id, _file, _path);

select 's3: many archives';
SELECT id, data, _size, _file, _path FROM s3(s3_conn, filename='03036_archive*.zip :: example*.csv') ORDER BY (id, _file, _path);
select 's3Cluster: many archives';
SELECT id, data, _size, _file, _path FROM s3Cluster('test_cluster_two_shards', s3_conn, filename='03036_archive*.zip :: example*.csv') ORDER BY (id, _file, _path);
