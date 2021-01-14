create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by x set y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by b set y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b, x set y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a set b = min(b), y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set y = max(y), y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), a) ttl d + interval 1 second group by toDate(d), a set d = min(d), b = max(b); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (d, -(a + b)) ttl d + interval 1 second group by d, -(a + b) set a = sum(a), b = min(b); -- { serverError 450}
