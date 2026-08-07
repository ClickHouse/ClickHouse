-- Tags: no-fasttest

drop table if exists test;

create table test engine=S3('http://localhost:11111/test/json_data');
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', NOSIGN);
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', auto);
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', auto, 'none');
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', NOSIGN, auto);
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', 'test', 'testtest');
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', NOSIGN, auto, 'none');
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', 'test', 'testtest', '');
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', 'test', 'testtest', '', auto);
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', 'test', 'testtest', auto, 'none');
show create table test;
drop table test;

create table test engine=S3('http://localhost:11111/test/json_data', 'test', 'testtest', '', auto, 'none');
show create table test;
drop table test;

create table test engine=URL('http://localhost:11111/test/json_data');
show create table test;
drop table test;

create table test engine=URL('http://localhost:11111/test/json_data', auto);
show create table test;
drop table test;

create table test engine=URL('http://localhost:11111/test/json_data', auto, 'none');
show create table test;
drop table test;
