drop table if exists perf_lc_num;

CREATE TABLE perf_lc_num(　        num UInt8,　        arr Array(LowCardinality(Int64)) default [num]　        ) ENGINE = TinyLog;

INSERT INTO perf_lc_num (num) SELECT toUInt8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

INSERT INTO perf_lc_num (num) SELECT toUInt8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

drop table if exists perf_lc_num;


CREATE TABLE perf_lc_num(　        num UInt8,　        arr Array(LowCardinality(Int64)) default [num]　        ) ENGINE = Log;

INSERT INTO perf_lc_num (num) SELECT toUInt8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

INSERT INTO perf_lc_num (num) SELECT toUInt8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

drop table if exists perf_lc_num;


CREATE TABLE perf_lc_num(　        num UInt8,　        arr Array(LowCardinality(Int64)) default [num]　        ) ENGINE = StripeLog;

INSERT INTO perf_lc_num (num) SELECT toUInt8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

INSERT INTO perf_lc_num (num) SELECT toUInt8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

drop table if exists perf_lc_num;


