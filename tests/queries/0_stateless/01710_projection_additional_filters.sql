DROP TABLE IF EXISTS t;

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

CREATE TABLE t(a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY a ORDER BY a;

INSERT INTO t SELECT number % 10, number FROM numbers(10000);

SELECT count(), min(a), max(a) FROM t SETTINGS additional_table_filters = {'t' : '0'};

DROP TABLE t;

drop table if exists atf_p;
create table atf_p (x UInt64) engine = MergeTree order by tuple();
insert into atf_p select number from numbers(10);
select count() from atf_p settings additional_table_filters = {'atf_p': 'x <= 2'};
drop table atf_p;
