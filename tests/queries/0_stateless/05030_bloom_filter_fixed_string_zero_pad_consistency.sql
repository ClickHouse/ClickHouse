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

-- Every skip index that answers an array-search function has to agree with it, not just
-- `bloom_filter`. `ngrambf_v1` and a `text` index with a term-preserving tokenizer keep 'V0' and
-- 'V0\0' as distinct terms, so a single lookup for a `FixedString` needle cannot find both and the
-- index has to decline. Ids 0, 1 and 2 are one value under the zero-padding rule, so all three
-- match; each cell compares the indexed answer against an unindexed table over the same data.
drop table if exists plain_arr;
drop table if exists ng_arr;
drop table if exists tk_arr;
drop table if exists tx_arr;
drop table if exists ng_map;
drop table if exists tx_map;

create table plain_arr (id UInt64, v Array(String)) engine = Log;
create table ng_arr (id UInt64, v Array(String), index idx v type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table tk_arr (id UInt64, v Array(String), index idx v type tokenbf_v1(512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table tx_arr (id UInt64, v Array(String), index idx v type text(tokenizer = array) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_arr values (0, ['V0']), (1, ['V0\0']), (2, ['V0\0\0']), (3, ['X']);
insert into ng_arr values (0, ['V0']), (1, ['V0\0']), (2, ['V0\0\0']), (3, ['X']);
insert into tk_arr values (0, ['V0']), (1, ['V0\0']), (2, ['V0\0\0']), (3, ['X']);
insert into tx_arr values (0, ['V0']), (1, ['V0\0']), (2, ['V0\0\0']), (3, ['X']);

select 'other skip indexes must not change the result';
select 'ngrambf has',
    (select groupArray(id) from (select id from plain_arr where has(v, toFixedString('V0', 3)) order by id))
  = (select groupArray(id) from (select id from ng_arr where has(v, toFixedString('V0', 3)) order by id));
select 'ngrambf hasAny',
    (select groupArray(id) from (select id from plain_arr where hasAny(v, [toFixedString('V0', 3)]) order by id))
  = (select groupArray(id) from (select id from ng_arr where hasAny(v, [toFixedString('V0', 3)]) order by id));
select 'ngrambf hasAll',
    (select groupArray(id) from (select id from plain_arr where hasAll(v, [toFixedString('V0', 3)]) order by id))
  = (select groupArray(id) from (select id from ng_arr where hasAll(v, [toFixedString('V0', 3)]) order by id));
select 'tokenbf has',
    (select groupArray(id) from (select id from plain_arr where has(v, toFixedString('V0', 3)) order by id))
  = (select groupArray(id) from (select id from tk_arr where has(v, toFixedString('V0', 3)) order by id));
select 'text has',
    (select groupArray(id) from (select id from plain_arr where has(v, toFixedString('V0', 3)) order by id))
  = (select groupArray(id) from (select id from tx_arr where has(v, toFixedString('V0', 3)) order by id));
select 'text hasAny',
    (select groupArray(id) from (select id from plain_arr where hasAny(v, [toFixedString('V0', 3)]) order by id))
  = (select groupArray(id) from (select id from tx_arr where hasAny(v, [toFixedString('V0', 3)]) order by id));
select 'text hasAll',
    (select groupArray(id) from (select id from plain_arr where hasAll(v, [toFixedString('V0', 3)]) order by id))
  = (select groupArray(id) from (select id from tx_arr where hasAll(v, [toFixedString('V0', 3)]) order by id));
-- Pin the rows too: a keyed-vs-unkeyed comparison alone stays green if a change moves both sides.
select 'matching rows are 0 1 2',
    (select groupArray(id) from (select id from ng_arr where has(v, toFixedString('V0', 3)) order by id)) = [0, 1, 2];

-- An index over a Map subcolumn reaches the same functions through `mapContains`.
create table ng_map (id UInt64, m Map(String, UInt8), index idx mapKeys(m) type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table tx_map (id UInt64, m Map(String, UInt8), index idx mapKeys(m) type text(tokenizer = array) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
insert into ng_map values (0, map('V0', 1)), (1, map('V0\0', 1)), (2, map('V0\0\0', 1)), (3, map('X', 1));
insert into tx_map values (0, map('V0', 1)), (1, map('V0\0', 1)), (2, map('V0\0\0', 1)), (3, map('X', 1));

select 'map subcolumn indexes';
select 'ngrambf mapContains',
    (select groupArray(id) from (select id from ng_map where mapContains(m, toFixedString('V0', 3)) order by id))
  = [0, 1, 2];
select 'text mapContains',
    (select groupArray(id) from (select id from tx_map where mapContains(m, toFixedString('V0', 3)) order by id))
  = [0, 1, 2];

-- A plain `String` needle involves no padding, so these indexes must still prune normally.
select 'string needle still uses the index';
select 'ngrambf has Str',
    (select groupArray(id) from (select id from plain_arr where has(v, 'V0') order by id))
  = (select groupArray(id) from (select id from ng_arr where has(v, 'V0') order by id));
select 'text has Str',
    (select groupArray(id) from (select id from plain_arr where has(v, 'V0') order by id))
  = (select groupArray(id) from (select id from tx_arr where has(v, 'V0') order by id));

drop table plain_arr;
drop table ng_arr;
drop table tk_arr;
drop table tx_arr;
drop table ng_map;
drop table tx_map;
