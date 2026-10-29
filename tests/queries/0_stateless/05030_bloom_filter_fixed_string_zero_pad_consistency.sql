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
    (select arraySort(groupArray(id)) from (select id from plain_str where has([toFixedString('V0', 3)], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id));
select 'fs5 needle',
    (select arraySort(groupArray(id)) from (select id from plain_str where has([toFixedString('V0', 5)], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_str where has([toFixedString('V0', 5)], s) order by id));
select 'str needle',
    (select arraySort(groupArray(id)) from (select id from plain_str where has(['V0'], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_str where has(['V0'], s) order by id));

-- Skipping the index must not change the result either.
select 'skip index setting must not change the result';
select 'fs3 needle',
    (select arraySort(groupArray(id)) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id) settings use_skip_indexes = 0);

-- The rows themselves, so a regression shows which rows were lost rather than only that a
-- comparison failed. Under `equals` padding semantics ids 0, 1 and 2 all match a
-- `FixedString` needle spelling 'V0', while a plain `String` needle 'V0' matches only id 0.
select 'matching rows';
select 'fs3 needle', arraySort(groupArray(id)) from (select id from idx_str where has([toFixedString('V0', 3)], s) order by id);
select 'fs5 needle', arraySort(groupArray(id)) from (select id from idx_str where has([toFixedString('V0', 5)], s) order by id);
select 'str needle', arraySort(groupArray(id)) from (select id from idx_str where has(['V0'], s) order by id);

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
    (select arraySort(groupArray(id)) from (select id from plain_fs where has([toFixedString('V0', 5)], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_fs where has([toFixedString('V0', 5)], s) order by id));
select 'narrower needle',
    (select arraySort(groupArray(id)) from (select id from plain_fs where has([toFixedString('V0', 2)], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_fs where has([toFixedString('V0', 2)], s) order by id));
select 'exact needle',
    (select arraySort(groupArray(id)) from (select id from plain_fs where has([toFixedString('V0', 3)], s) order by id))
  = (select arraySort(groupArray(id)) from (select id from idx_fs where has([toFixedString('V0', 3)], s) order by id));

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
    (select arraySort(groupArray(id)) from (select id from plain_arr where has(v, toFixedString('V0', 3)) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_arr where has(v, toFixedString('V0', 3)) order by id));
select 'ngrambf hasAny',
    (select arraySort(groupArray(id)) from (select id from plain_arr where hasAny(v, [toFixedString('V0', 3)]) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_arr where hasAny(v, [toFixedString('V0', 3)]) order by id));
select 'ngrambf hasAll',
    (select arraySort(groupArray(id)) from (select id from plain_arr where hasAll(v, [toFixedString('V0', 3)]) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_arr where hasAll(v, [toFixedString('V0', 3)]) order by id));
select 'tokenbf has',
    (select arraySort(groupArray(id)) from (select id from plain_arr where has(v, toFixedString('V0', 3)) order by id))
  = (select arraySort(groupArray(id)) from (select id from tk_arr where has(v, toFixedString('V0', 3)) order by id));
select 'text has',
    (select arraySort(groupArray(id)) from (select id from plain_arr where has(v, toFixedString('V0', 3)) order by id))
  = (select arraySort(groupArray(id)) from (select id from tx_arr where has(v, toFixedString('V0', 3)) order by id));
select 'text hasAny',
    (select arraySort(groupArray(id)) from (select id from plain_arr where hasAny(v, [toFixedString('V0', 3)]) order by id))
  = (select arraySort(groupArray(id)) from (select id from tx_arr where hasAny(v, [toFixedString('V0', 3)]) order by id));
select 'text hasAll',
    (select arraySort(groupArray(id)) from (select id from plain_arr where hasAll(v, [toFixedString('V0', 3)]) order by id))
  = (select arraySort(groupArray(id)) from (select id from tx_arr where hasAll(v, [toFixedString('V0', 3)]) order by id));
-- Pin the rows too: a keyed-vs-unkeyed comparison alone stays green if a change moves both sides.
select 'matching rows are 0 1 2',
    (select arraySort(groupArray(id)) from (select id from ng_arr where has(v, toFixedString('V0', 3)) order by id)) = [0, 1, 2];

-- An index over a Map subcolumn reaches the same functions through `mapContains`.
create table ng_map (id UInt64, m Map(String, UInt8), index idx mapKeys(m) type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table tx_map (id UInt64, m Map(String, UInt8), index idx mapKeys(m) type text(tokenizer = array) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
insert into ng_map values (0, map('V0', 1)), (1, map('V0\0', 1)), (2, map('V0\0\0', 1)), (3, map('X', 1));
insert into tx_map values (0, map('V0', 1)), (1, map('V0\0', 1)), (2, map('V0\0\0', 1)), (3, map('X', 1));

select 'map subcolumn indexes';
select 'ngrambf mapContains',
    (select arraySort(groupArray(id)) from (select id from ng_map where mapContains(m, toFixedString('V0', 3)) order by id))
  = [0, 1, 2];
select 'text mapContains',
    (select arraySort(groupArray(id)) from (select id from tx_map where mapContains(m, toFixedString('V0', 3)) order by id))
  = [0, 1, 2];

-- A plain `String` needle involves no padding, so these indexes must still prune normally.
select 'string needle still uses the index';
select 'ngrambf has Str',
    (select arraySort(groupArray(id)) from (select id from plain_arr where has(v, 'V0') order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_arr where has(v, 'V0') order by id));
select 'text has Str',
    (select arraySort(groupArray(id)) from (select id from plain_arr where has(v, 'V0') order by id))
  = (select arraySort(groupArray(id)) from (select id from tx_arr where has(v, 'V0') order by id));

drop table plain_arr;
drop table ng_arr;
drop table tk_arr;
drop table tx_arr;
drop table ng_map;
drop table tx_map;

-- Over `FixedString` elements the needle's trailing zero bytes are padding, whether it is a `String` or a
-- wider `FixedString`, so `has(v, 'ab\0')` matches the element 'ab'. `ngrambf_v1` and `sparse_grams` must
-- not build their probe from those zero bytes, which the stored value does not have.
drop table if exists plain_fs_arr;
drop table if exists ng_fs_arr;
drop table if exists sg_fs_arr;
drop table if exists tk_fs_arr;
drop table if exists plain_fs_map;
drop table if exists ng_fs_map_keys;
drop table if exists ng_fs_map_values;

create table plain_fs_arr (id UInt64, v Array(FixedString(4))) engine = Log;
create table ng_fs_arr (id UInt64, v Array(FixedString(4)), index idx v type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table sg_fs_arr (id UInt64, v Array(FixedString(4)), index idx v type sparse_grams(3, 100, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table tk_fs_arr (id UInt64, v Array(FixedString(4)), index idx v type tokenbf_v1(512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_fs_arr values (0, ['abcd']), (1, ['ab']), (2, ['xyz']);
insert into ng_fs_arr values (0, ['abcd']), (1, ['ab']), (2, ['xyz']);
insert into sg_fs_arr values (0, ['abcd']), (1, ['ab']), (2, ['xyz']);
insert into tk_fs_arr values (0, ['abcd']), (1, ['ab']), (2, ['xyz']);

select 'fixed string array elements';
select 'scan has str', (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, 'abcd\0') order by id)) = [0];
select 'scan has short str', (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, 'ab\0') order by id)) = [1];
select 'ngrambf has str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, 'abcd\0') order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_arr where has(v, 'abcd\0') order by id));
select 'ngrambf has short str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, 'ab\0') order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_arr where has(v, 'ab\0') order by id));
select 'ngrambf has wider fs',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, toFixedString('abcd', 8)) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_arr where has(v, toFixedString('abcd', 8)) order by id));
select 'ngrambf hasAny str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where hasAny(v, ['abcd\0']) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_arr where hasAny(v, ['abcd\0']) order by id));
select 'sparse_grams has str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, 'abcd\0') order by id))
  = (select arraySort(groupArray(id)) from (select id from sg_fs_arr where has(v, 'abcd\0') order by id));
select 'sparse_grams has wider fs',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, toFixedString('abcd', 8)) order by id))
  = (select arraySort(groupArray(id)) from (select id from sg_fs_arr where has(v, toFixedString('abcd', 8)) order by id));
select 'tokenbf has str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_arr where has(v, 'abcd\0') order by id))
  = (select arraySort(groupArray(id)) from (select id from tk_fs_arr where has(v, 'abcd\0') order by id));

create table plain_fs_map (id UInt64, k Map(FixedString(4), UInt8), w Map(String, FixedString(4))) engine = Log;
create table ng_fs_map_keys (id UInt64, k Map(FixedString(4), UInt8), index idx mapKeys(k) type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table ng_fs_map_values (id UInt64, w Map(String, FixedString(4)), index idx mapValues(w) type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_fs_map values (0, map('abcd', 1), map('x', 'abcd')), (1, map('xyz', 1), map('x', 'xyz'));
insert into ng_fs_map_keys values (0, map('abcd', 1)), (1, map('xyz', 1));
insert into ng_fs_map_values values (0, map('x', 'abcd')), (1, map('x', 'xyz'));

select 'ngrambf mapContainsKey str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_map where mapContainsKey(k, 'abcd\0') order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_map_keys where mapContainsKey(k, 'abcd\0') order by id));
select 'ngrambf mapContainsValue str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_map where mapContainsValue(w, 'abcd\0') order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_map_values where mapContainsValue(w, 'abcd\0') order by id));

drop table plain_fs_arr;
drop table ng_fs_arr;
drop table sg_fs_arr;
drop table tk_fs_arr;
drop table plain_fs_map;
drop table ng_fs_map_keys;
drop table ng_fs_map_values;

-- Removing the needle's trailing zero bytes is not enough: the element `unhex('6162C2')` of `FixedString(4)` is stored
-- as `ab\xC2\0`, and `ngrams` reads `\xC2\0` as one UTF-8 sequence, but `\xC2` without the zero byte as another one,
-- so the probe had a gram the stored value does not have. The probe is built from the needle padded to the element width.
drop table if exists plain_fs_bin;
drop table if exists ng_fs_bin;
drop table if exists sg_fs_bin;
drop table if exists plain_fs_bin_map;
drop table if exists ng_fs_bin_map_keys;

create table plain_fs_bin (id UInt64, v Array(FixedString(4))) engine = Log;
create table ng_fs_bin (id UInt64, v Array(FixedString(4)), index idx v type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;
create table sg_fs_bin (id UInt64, v Array(FixedString(4)), index idx v type sparse_grams(3, 100, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_fs_bin select number, [toFixedString(unhex(['6162C2', '78797A'][number + 1]), 4)] from numbers(2);
insert into ng_fs_bin select number, [toFixedString(unhex(['6162C2', '78797A'][number + 1]), 4)] from numbers(2);
insert into sg_fs_bin select number, [toFixedString(unhex(['6162C2', '78797A'][number + 1]), 4)] from numbers(2);

select 'incomplete UTF-8 sequence before the padding';
select 'scan has wider fs', (select arraySort(groupArray(id)) from (select id from plain_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id)) = [0];
select 'ngrambf has wider fs',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id));
select 'ngrambf has wider fs, skip indexes off',
    (select arraySort(groupArray(id)) from (select id from ng_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id settings use_skip_indexes = 0))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id));
select 'ngrambf has str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin where has(v, unhex('6162C2')) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin where has(v, unhex('6162C2')) order by id));
select 'ngrambf has str with zero',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin where has(v, unhex('6162C200')) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin where has(v, unhex('6162C200')) order by id));
select 'ngrambf hasAny str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin where hasAny(v, [unhex('6162C2')]) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin where hasAny(v, [unhex('6162C2')]) order by id));
select 'ngrambf hasAll str',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin where hasAll(v, [unhex('6162C2')]) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin where hasAll(v, [unhex('6162C2')]) order by id));
select 'sparse_grams has wider fs',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id))
  = (select arraySort(groupArray(id)) from (select id from sg_fs_bin where has(v, toFixedString(unhex('6162C2'), 8)) order by id));

create table plain_fs_bin_map (id UInt64, k Map(FixedString(4), UInt8)) engine = Log;
create table ng_fs_bin_map_keys (id UInt64, k Map(FixedString(4), UInt8), index idx mapKeys(k) type ngrambf_v1(3, 512, 2, 0) granularity 1)
    engine = MergeTree order by id settings index_granularity = 1;

insert into plain_fs_bin_map select number, map(toFixedString(unhex(['6162C2', '78797A'][number + 1]), 4), 1) from numbers(2);
insert into ng_fs_bin_map_keys select number, map(toFixedString(unhex(['6162C2', '78797A'][number + 1]), 4), 1) from numbers(2);

select 'ngrambf mapContainsKey bin',
    (select arraySort(groupArray(id)) from (select id from plain_fs_bin_map where mapContainsKey(k, unhex('6162C2')) order by id))
  = (select arraySort(groupArray(id)) from (select id from ng_fs_bin_map_keys where mapContainsKey(k, unhex('6162C2')) order by id));

drop table plain_fs_bin;
drop table ng_fs_bin;
drop table sg_fs_bin;
drop table plain_fs_bin_map;
drop table ng_fs_bin_map_keys;
