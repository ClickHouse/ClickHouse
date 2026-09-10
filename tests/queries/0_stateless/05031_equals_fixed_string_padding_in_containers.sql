-- `equals` treats the trailing zero bytes of a `FixedString` as padding, so
-- `toFixedString('V0', 3) = 'V0\0'`. The rule reaches a `FixedString` nested in an `Array`, a
-- `Map` or a `Tuple`, so a container holding one compares equal to a container holding the same
-- value written out with its trailing zeros. Plain `String` against plain `String` stays
-- length-sensitive at every depth.

select 'scalar and tuple reference';
select 'fs3 = str_padded', toFixedString('V0', 3) = 'V0\0';
select 'tuple', tuple(toFixedString('V0', 3)) = tuple('V0\0');
select 'str = str_padded', 'V0' = 'V0\0';

select 'containers';
select 'array', [toFixedString('V0', 3)] = ['V0\0'];
select 'array str_short', [toFixedString('V0', 3)] = ['V0'];
select 'array of tuple', [tuple(toFixedString('V0', 3))] = [tuple('V0\0')];
select 'nested array', [[toFixedString('V0', 3)]] = [['V0\0']];
select 'map key', map(toFixedString('V0', 3), 1) = map('V0\0', 1);
select 'map value', map(1, toFixedString('V0', 3)) = map(1, 'V0\0');
select 'array of map', [map(toFixedString('V0', 3), 1)] = [map('V0\0', 1)];

-- The width of the `FixedString` is not part of the value.
select 'widths';
select 'fs3 vs fs4', [toFixedString('V0', 3)] = [toFixedString('V0', 4)];
select 'fs4 vs str_padded', [toFixedString('V0', 4)] = ['V0\0'];
select 'str element, fs needle', ['V0'] = [toFixedString('V0', 3)];

-- Every comparison operator shares one code path and must agree.
select 'operators';
select 'eq', [toFixedString('V0', 3)] = ['V0\0'];
select 'ne', [toFixedString('V0', 3)] != ['V0\0'];
select 'lt', [toFixedString('V0', 3)] < ['V0\0'];
select 'le', [toFixedString('V0', 3)] <= ['V0\0'];
select 'gt', [toFixedString('V0', 3)] > ['V0\0'];
select 'ge', [toFixedString('V0', 3)] >= ['V0\0'];

-- Constant folding must not disagree with the vector path.
select 'materialized';
select 'array', [toFixedString('V0', 3)] = materialize(['V0\0']);
select 'array both', materialize([toFixedString('V0', 3)]) = materialize(['V0\0']);
select 'map', map(toFixedString('V0', 3), 1) = materialize(map('V0\0', 1));
select 'nested array', materialize([[toFixedString('V0', 3)]]) = [['V0\0']];

select 'low cardinality and nullable';
select 'lc element', [toLowCardinality(toFixedString('V0', 3))] = [toLowCardinality('V0\0')];
select 'lc materialized', materialize([toLowCardinality(toFixedString('V0', 3))]) = [toLowCardinality('V0\0')];
select 'nullable element',
    cast([toFixedString('V0', 3)], 'Array(Nullable(FixedString(3)))') = cast(['V0\0'], 'Array(Nullable(String))');
select 'null stays null', cast([null], 'Array(Nullable(FixedString(3)))') = cast(['V0\0'], 'Array(Nullable(String))');

-- Only trailing zeros of a `FixedString` are padding.
select 'negative controls';
select 'str vs str_padded', ['V0'] = ['V0\0'];
select 'str prefix', ['ab'] = ['abc'];
select 'interior zero preserved', [toFixedString('a\0b', 3)] = [toFixedString('a\0b', 4)];
select 'interior zero not collapsed', [toFixedString('a\0b', 3)] = [toFixedString('ab', 3)];
select 'different content', [toFixedString('AB', 2)] = [toFixedString('AC', 2)];
select 'different length arrays', [toFixedString('V0', 3)] = ['V0\0', 'x'];
select 'element order', [toFixedString('V0', 3), toFixedString('q', 1)] = ['q', 'V0\0'];

-- Values longer than one SIMD register.
select 'long values';
select 'long match', [toFixedString(repeat('a', 33), 33)] = [toFixedString(repeat('a', 33), 40)];
select 'long no match', [toFixedString(repeat('a', 33), 33)] = [toFixedString(repeat('b', 33), 40)];

-- `has` searches for an element with the same comparison, so the two must agree.
select 'agreement with has and arrayExists';
select 'nested has',
    has([[toFixedString('V0', 3)]], ['V0\0']),
    has(materialize([[toFixedString('V0', 3)]]), ['V0\0']),
    arrayExists(x -> x = ['V0\0'], [[toFixedString('V0', 3)]]);
select 'nested indexOf',
    indexOf([[toFixedString('V0', 3)]], ['V0\0']),
    indexOf(materialize([[toFixedString('V0', 3)]]), ['V0\0']);
select 'map element has',
    has([map(toFixedString('V0', 3), 1)], map('V0\0', 1)),
    has(materialize([map(toFixedString('V0', 3), 1)]), map('V0\0', 1));
-- `toFixedString('V0', 3)` is stored as the three bytes `V0\0`, which are byte-identical to the
-- `String` `'V0\0'`. Comparing against `'V0'` instead is the case a byte-wise comparison gets
-- wrong, so it is the one that distinguishes a working search from a coincidence.
select 'short needle nested array',
    has([[toFixedString('V0', 3)]], ['V0']),
    has(materialize([[toFixedString('V0', 3)]]), ['V0']),
    arrayExists(x -> x = ['V0'], [[toFixedString('V0', 3)]]);
select 'short needle map',
    has([map(toFixedString('V0', 3), 1)], map('V0', 1)),
    has(materialize([map(toFixedString('V0', 3), 1)]), map('V0', 1)),
    arrayExists(x -> x = map('V0', 1), [map(toFixedString('V0', 3), 1)]);
select 'short needle array of tuple',
    has([[tuple(toFixedString('V0', 3))]], [tuple('V0')]),
    has(materialize([[tuple(toFixedString('V0', 3))]]), [tuple('V0')]);

-- The matching values are not a contiguous run of the sort order: `['V0', 'a']` lies between
-- `['V0']` and `['V0\0']` and does not match. A range built from the constant would prune a
-- granule holding a matching row, so index analysis is declined for these comparisons.
select 'primary key must not prune matching rows';
create table t_zero_pad_key (a Array(String)) engine = MergeTree order by a settings index_granularity = 1;
insert into t_zero_pad_key values (['V0']), (['V0', 'a']), (['V0\0']), (['ZZ']);
select 'index path', count() from t_zero_pad_key where a = cast(['V0'] as Array(FixedString(3)));
select 'full scan path', count() from t_zero_pad_key where a = materialize(cast(['V0'] as Array(FixedString(3))));
select 'exact string constant still prunes', count() from t_zero_pad_key where a = ['V0\0'];
drop table t_zero_pad_key;
