-- Tags: no-parallel

create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by x set y = max(y); -- { serverError BAD_TTL_EXPRESSION}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by b set y = max(y); -- { serverError BAD_TTL_EXPRESSION}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b, x set y = max(y); -- { serverError BAD_TTL_EXPRESSION}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set y = max(y), y = max(y); -- { serverError BAD_TTL_EXPRESSION}
