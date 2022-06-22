-- Tags: no-fasttest, use-hdfs, no-backward-compatibility-check:22.5

create table test_02311 (x UInt32) engine=File(UnknownFormat); -- {serverError UNKNOWN_FORMAT}
create table test_02311 (x UInt32) engine=URL('http://some/url', UnknownFormat); -- {serverError UNKNOWN_FORMAT}
create table test_02311 (x UInt32) engine=S3('http://host:2020/test/data', UnknownFormat); -- {serverError UNKNOWN_FORMAT}
create table test_02311 (x UInt32) engine=HDFS('http://hdfs:9000/data', UnknownFormat); -- {serverError UNKNOWN_FORMAT}
