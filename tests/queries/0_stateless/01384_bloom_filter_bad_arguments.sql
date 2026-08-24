DROP TABLE IF EXISTS test;

create table test (a String, index a a type tokenbf_v1(0, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }
create table test (a String, index a a type tokenbf_v1(2, 0, 0) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }
create table test (a String, index a a type tokenbf_v1(0, 1, 1) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }
create table test (a String, index a a type tokenbf_v1(1, 0, 1) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }

create table test (a String, index a a type tokenbf_v1(0.1, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }
create table test (a String, index a a type tokenbf_v1(-1, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }
create table test (a String, index a a type tokenbf_v1(0xFFFFFFFF, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError BAD_ARGUMENTS }
