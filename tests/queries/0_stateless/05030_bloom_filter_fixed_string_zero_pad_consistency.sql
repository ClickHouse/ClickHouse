-- https://github.com/ClickHouse/ClickHouse/issues/115311
--
-- `has(<constant array>, <indexed column>)` can be answered from a `bloom_filter` index, which
-- probes one hash per constant element. Once the comparison treats trailing zero bytes of a
-- `FixedString` as padding, one logical value has several valid encodings on disk ('V0',
-- 'V0\0', 'V0\0\0'), and a single hash cannot match all of them. The index must therefore
-- either account for that or decline, never prune a row the predicate would accept.
--
-- Each query compares the indexed table against an identical unindexed one, so the assertions
-- hold whatever the padding rule turns out to be: adding an index must not change results.

drop table if exists plain_str;
drop table if exists idx_str;

create table plain_str (id UInt64, s String) engine = Log;
create table idx_str (id UInt64, s String, index idx s type bloom_filter granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_str values (0, 'V0'), (1, 'V0\0'), (2, 'V0\0\0'), (3, 'X');
insert into idx_str values (0, 'V0'), (1, 'V0\0'), (2, 'V0\0\0'), (3, 'X');

select 'index must not change the result';
select 'fs3 needle',
    (select groupArray(id) from (select id from plain_str where has([toFixedString('V0', 3)], s) order by id))
  = (select groupArray(id) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id));
select 'fs5 needle',
    (select groupArray(id) from (select id from plain_str where has([toFixedString('V0', 5)], s) order by id))
  = (select groupArray(id) from (select id from idx_str where has([toFixedString('V0', 5)], s) order by id));
select 'str needle',
    (select groupArray(id) from (select id from plain_str where has(['V0'], s) order by id))
  = (select groupArray(id) from (select id from idx_str where has(['V0'], s) order by id));

-- Skipping the index must not change the result either.
select 'skip index setting must not change the result';
select 'fs3 needle',
    (select groupArray(id) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id))
  = (select groupArray(id) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id) settings use_skip_indexes = 0);

-- The rows themselves, so a regression shows which rows were lost rather than only that a
-- comparison failed. Under `equals` padding semantics ids 0, 1 and 2 all match a
-- `FixedString` needle spelling 'V0', while a plain `String` needle 'V0' matches only id 0.
select 'matching rows';
select 'fs3 needle', groupArray(id) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id);
select 'fs5 needle', groupArray(id) from (select id from idx_str where has([toFixedString('V0', 5)], s) order by id);
select 'str needle', groupArray(id) from (select id from idx_str where has(['V0'], s) order by id);

drop table plain_str;
drop table idx_str;

-- A `FixedString` column has exactly one encoding per value, so its index stays usable.
drop table if exists plain_fs;
drop table if exists idx_fs;

create table plain_fs (id UInt64, s FixedString(3)) engine = Log;
create table idx_fs (id UInt64, s FixedString(3), index idx s type bloom_filter granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_fs values (0, 'V0'), (1, 'X');
insert into idx_fs values (0, 'V0'), (1, 'X');

select 'fixed string column';
select 'wider needle',
    (select groupArray(id) from (select id from plain_fs where has([toFixedString('V0', 5)], s) order by id))
  = (select groupArray(id) from (select id from idx_fs where has([toFixedString('V0', 5)], s) order by id));
select 'narrower needle',
    (select groupArray(id) from (select id from plain_fs where has([toFixedString('V0', 2)], s) order by id))
  = (select groupArray(id) from (select id from idx_fs where has([toFixedString('V0', 2)], s) order by id));
select 'exact needle',
    (select groupArray(id) from (select id from plain_fs where has([toFixedString('V0', 3)], s) order by id))
  = (select groupArray(id) from (select id from idx_fs where has([toFixedString('V0', 3)], s) order by id));

drop table plain_fs;
drop table idx_fs;
