drop table if exists test54378;
set allow_deprecated_syntax_for_merge_tree=1;
create table test54378 (part_date Date, pk_date Date, date Date) Engine=MergeTree(part_date, pk_date, 8192);
insert into test54378 values ('2018-04-19', '2018-04-19', '2018-04-19');

select 111 from test54378 where part_date = '2018-04-19';
select 112 from test54378 where part_date in ('2018-04-19');
select 113 from test54378 where pk_date in ('2018-04-19');
select 114 from test54378 where date in ('2018-04-19');
SELECT '-';
select 121 from test54378 where part_date = toDate('2018-04-19');
select 122 from test54378 where part_date in (toDate('2018-04-19'));
select 123 from test54378 where pk_date in (toDate('2018-04-19'));
select 124 from test54378 where date in (toDate('2018-04-19'));
SELECT '-';
select 131 from test54378 where part_date = (SELECT toDate('2018-04-19'));
select 132 from test54378 where part_date in (SELECT toDate('2018-04-19'));
select 133 from test54378 where pk_date in (SELECT toDate('2018-04-19'));
select 134 from test54378 where date in (SELECT toDate('2018-04-19'));

SELECT '---';

select 211 from test54378 prewhere part_date = '2018-04-19';
select 212 from test54378 prewhere part_date in ('2018-04-19');
select 213 from test54378 prewhere pk_date in ('2018-04-19');
select 214 from test54378 prewhere date in ('2018-04-19');
SELECT '-';
select 221 from test54378 prewhere part_date = toDate('2018-04-19');
select 222 from test54378 prewhere part_date in (toDate('2018-04-19'));
select 223 from test54378 prewhere pk_date in (toDate('2018-04-19'));
select 224 from test54378 prewhere date in (toDate('2018-04-19'));
SELECT '-';
select 231 from test54378 prewhere part_date = (SELECT toDate('2018-04-19'));
select 232 from test54378 prewhere part_date in (SELECT toDate('2018-04-19'));
select 233 from test54378 prewhere pk_date in (SELECT toDate('2018-04-19'));
select 234 from test54378 prewhere date in (SELECT toDate('2018-04-19'));

SELECT '---';

SET optimize_move_to_prewhere = 0;

select 311 from test54378 where part_date = '2018-04-19';
select 312 from test54378 where part_date in ('2018-04-19');
select 313 from test54378 where pk_date in ('2018-04-19');
select 314 from test54378 where date in ('2018-04-19');
SELECT '-';
select 321 from test54378 where part_date = toDate('2018-04-19');
select 322 from test54378 where part_date in (toDate('2018-04-19'));
select 323 from test54378 where pk_date in (toDate('2018-04-19'));
select 324 from test54378 where date in (toDate('2018-04-19'));
SELECT '-';
select 331 from test54378 where part_date = (SELECT toDate('2018-04-19'));
select 332 from test54378 where part_date in (SELECT toDate('2018-04-19'));
select 333 from test54378 where pk_date in (SELECT toDate('2018-04-19'));
select 334 from test54378 where date in (SELECT toDate('2018-04-19'));

SELECT '---';

SET optimize_move_to_prewhere = 1;

select 411 from test54378 where part_date = '2018-04-19';
select 412 from test54378 where part_date in ('2018-04-19');
select 413 from test54378 where pk_date in ('2018-04-19');
select 414 from test54378 where date in ('2018-04-19');
SELECT '-';
select 421 from test54378 where part_date = toDate('2018-04-19');
select 422 from test54378 where part_date in (toDate('2018-04-19'));
select 423 from test54378 where pk_date in (toDate('2018-04-19'));
select 424 from test54378 where date in (toDate('2018-04-19'));
SELECT '-';
select 431 from test54378 where part_date = (SELECT toDate('2018-04-19'));
select 432 from test54378 where part_date in (SELECT toDate('2018-04-19'));
select 433 from test54378 where pk_date in (SELECT toDate('2018-04-19'));
select 434 from test54378 where date in (SELECT toDate('2018-04-19'));

drop table test54378;
