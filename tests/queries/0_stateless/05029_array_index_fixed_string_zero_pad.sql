-- https://github.com/ClickHouse/ClickHouse/issues/115311
--
-- `equals` treats trailing zero bytes of a `FixedString` as padding, so
-- `toFixedString('V0', 3) = toFixedString('V0', 4)` and `toFixedString('V0', 3) = 'V0\0'`.
-- `has`, `indexOf`, `countEqual`, `indexOfAssumeSorted`, `hasAny`, `hasAll` and `hasSubstr` must
-- agree with `equals`, and must give the same answer whether the array argument is constant or
-- materialized.
--
-- Every expected value below is the value `arrayExists` / `arrayFirstIndex` / `arrayCount`
-- already return for the same operands. Each row prints the constant and the materialized
-- form side by side, so a mismatch identifies the failing path.

select 'equals reference';
select 'fs3 = fs4', equals(toFixedString('V0', 3), toFixedString('V0', 4));
select 'fs3 = str_padded', equals(toFixedString('V0', 3), 'V0\0');
select 'fs3 = str_short', equals(toFixedString('V0', 3), 'V0');
select 'str = str_padded', equals('V0', 'V0\0');

-- The three reproducers from the issue, both spellings of each.
select 'issue repro';
select 'has fs4 needle',
    has([toFixedString('V0', 3)], toFixedString('V0', 4)),
    has(materialize([toFixedString('V0', 3)]), toFixedString('V0', 4));
select 'has str_padded needle',
    has([toFixedString('V0', 3)], 'V0\0'),
    has(materialize([toFixedString('V0', 3)]), 'V0\0');
select 'indexOf fs4 needle',
    indexOf([toFixedString('V0', 3)], toFixedString('V0', 4)),
    indexOf(materialize([toFixedString('V0', 3)]), toFixedString('V0', 4));
-- The issue also reports the tuple form.
select 'has tuple fs4 needle',
    has([tuple(toFixedString('V0', 3))], tuple(toFixedString('V0', 4))),
    has(materialize([tuple(toFixedString('V0', 3))]), tuple(toFixedString('V0', 4)));
select 'has tuple str_padded needle',
    has([tuple(toFixedString('V0', 3))], tuple('V0\0')),
    has(materialize([tuple(toFixedString('V0', 3))]), tuple('V0\0'));

-- Padding applies whichever side is wider, and whichever side is constant.
select 'width directions';
select 'has wide element', has([toFixedString('V0', 4)], toFixedString('V0', 3)),
    has(materialize([toFixedString('V0', 4)]), toFixedString('V0', 3));
select 'indexOf wide element', indexOf([toFixedString('V0', 4)], toFixedString('V0', 3)),
    indexOf(materialize([toFixedString('V0', 4)]), toFixedString('V0', 3));
select 'has str_short needle', has([toFixedString('V0', 3)], 'V0'),
    has(materialize([toFixedString('V0', 3)]), 'V0');
select 'has materialized needle', has([toFixedString('V0', 3)], materialize(toFixedString('V0', 4))),
    has(materialize([toFixedString('V0', 3)]), materialize(toFixedString('V0', 4)));
select 'indexOf materialized needle', indexOf([toFixedString('V0', 3)], materialize(toFixedString('V0', 4))),
    indexOf(materialize([toFixedString('V0', 3)]), materialize(toFixedString('V0', 4)));

-- A `String` array element against a `FixedString` needle pads too -- `equals` says
-- `'ab' = toFixedString('ab', 3)`, so `has` must not disagree on either path.
select 'string element, fixed string needle';
select 'has str elem fs3 needle', has(['ab'], toFixedString('ab', 3)),
    has(materialize(['ab']), toFixedString('ab', 3));
select 'has str elem fs5 needle', has(['ab'], toFixedString('ab', 5)),
    has(materialize(['ab']), toFixedString('ab', 5));

-- Nullable carriers of the same element and needle types.
select 'nullable';
select 'has nullable element',
    has(cast([toFixedString('V0', 3), null], 'Array(Nullable(FixedString(3)))'), toFixedString('V0', 4)),
    has(materialize(cast([toFixedString('V0', 3), null], 'Array(Nullable(FixedString(3)))')), toFixedString('V0', 4));
select 'has nullable needle',
    has([toFixedString('V0', 3)], toNullable(toFixedString('V0', 4))),
    has(materialize([toFixedString('V0', 3)]), toNullable(toFixedString('V0', 4)));

-- countEqual shares the same comparison and must count every padded match.
select 'countEqual';
select 'countEqual single', countEqual([toFixedString('V0', 3)], toFixedString('V0', 4)),
    countEqual(materialize([toFixedString('V0', 3)]), toFixedString('V0', 4));
select 'countEqual duplicates',
    countEqual([toFixedString('V0', 3), toFixedString('V0', 3)], toFixedString('V0', 4)),
    countEqual(materialize([toFixedString('V0', 3), toFixedString('V0', 3)]), toFixedString('V0', 4));
select 'countEqual str_padded needle', countEqual([toFixedString('V0', 3)], 'V0\0'),
    countEqual(materialize([toFixedString('V0', 3)]), 'V0\0');
select 'countEqual no match', countEqual([toFixedString('AB', 2)], toFixedString('AC', 2)),
    countEqual(materialize([toFixedString('AB', 2)]), toFixedString('AC', 2));

-- indexOfAssumeSorted takes a different search path for constant arrays (binary search).
select 'indexOfAssumeSorted';
select 'indexOfAssumeSorted fs4 needle',
    indexOfAssumeSorted([toFixedString('V0', 3)], toFixedString('V0', 4)),
    indexOfAssumeSorted(materialize([toFixedString('V0', 3)]), toFixedString('V0', 4));

-- A Map is always converted to a full column, so it can never take the constant path.
-- It must still agree with the equivalent array spelling.
select 'map';
select 'has map fs4 needle', has(map(toFixedString('V0', 3), 1), toFixedString('V0', 4)),
    has(mapKeys(map(toFixedString('V0', 3), 1)), toFixedString('V0', 4));
select 'has map str_padded needle', has(map(toFixedString('V0', 3), 1), 'V0\0'),
    has(mapKeys(map(toFixedString('V0', 3), 1)), 'V0\0');
select 'has map string key', has(map('ab', 1), toFixedString('ab', 3)),
    has(mapKeys(map('ab', 1)), toFixedString('ab', 3));
select 'mapContains fs4 needle', mapContains(map(toFixedString('V0', 3), 1), toFixedString('V0', 4));

-- Position and multiplicity are preserved, not just presence.
select 'positions';
select 'indexOf second element',
    indexOf([toFixedString('q', 3), toFixedString('V0', 3)], toFixedString('V0', 4)),
    indexOf(materialize([toFixedString('q', 3), toFixedString('V0', 3)]), toFixedString('V0', 4));

-- Values longer than one SIMD register. `Field` holds these as heap-allocated strings with no
-- trailing padding, so a comparison that reads past the end of the value is caught here.
select 'long values';
select 'has long match',
    has([toFixedString(repeat('a', 33), 33)], toFixedString(repeat('a', 33), 40)),
    has(materialize([toFixedString(repeat('a', 33), 33)]), toFixedString(repeat('a', 33), 40));
select 'has long no match',
    has([toFixedString(repeat('a', 33), 33)], toFixedString(repeat('b', 33), 40)),
    has(materialize([toFixedString(repeat('a', 33), 33)]), toFixedString(repeat('b', 33), 40));

-- Negative controls. Only `FixedString` padding is ignored.
select 'negative controls';
select 'str vs str_padded must differ', has(['V0'], 'V0\0'),
    has(materialize(['V0']), 'V0\0');
select 'str prefix must differ', has(['ab'], 'abc'),
    has(materialize(['ab']), 'abc');
select 'same width different content', has([toFixedString('AB', 2)], toFixedString('AC', 2)),
    has(materialize([toFixedString('AB', 2)]), toFixedString('AC', 2));
select 'same width same content', has([toFixedString('AB', 2)], toFixedString('AB', 2)),
    has(materialize([toFixedString('AB', 2)]), toFixedString('AB', 2));
-- Only trailing zeros are padding; an interior zero byte is data.
select 'interior zero preserved',
    has([toFixedString('a\0b', 3)], toFixedString('a\0b', 4)),
    has(materialize([toFixedString('a\0b', 3)]), toFixedString('a\0b', 4));
select 'interior zero not collapsed',
    has([toFixedString('a\0b', 3)], toFixedString('ab', 3)),
    has(materialize([toFixedString('a\0b', 3)]), toFixedString('ab', 3));

-- A `Tuple` can pair a field the rule applies to with a `String`/`String` field where a trailing
-- zero byte is data. The rule is per field, not per tuple: the `FixedString` field below pads, the
-- `String` field does not, so `equals` keeps these tuples unequal and the search must too.
select 'mixed tuple, str field differs',
    has([tuple('a\0', toFixedString('b', 1))], tuple('a', toFixedString('b', 2))),
    has(materialize([tuple('a\0', toFixedString('b', 1))]), tuple('a', toFixedString('b', 2)));
select 'mixed tuple, str field equal',
    has([tuple('a', toFixedString('b', 1))], tuple('a', toFixedString('b', 2))),
    has(materialize([tuple('a', toFixedString('b', 1))]), tuple('a', toFixedString('b', 2)));
select 'mixed tuple, fs field differs',
    has([tuple('a', toFixedString('b', 1))], tuple('a', toFixedString('c', 2))),
    has(materialize([tuple('a', toFixedString('b', 1))]), tuple('a', toFixedString('c', 2)));
select 'mixed tuple nested one level',
    has([tuple('a\0', tuple(toFixedString('b', 1)))], tuple('a', tuple(toFixedString('b', 2)))),
    has(materialize([tuple('a\0', tuple(toFixedString('b', 1)))]), tuple('a', tuple(toFixedString('b', 2))));
-- The same tuple through the other functions, which canonicalise in their own code.
select 'mixed tuple hasAny',
    hasAny([tuple('a\0', toFixedString('b', 1))], [tuple('a', toFixedString('b', 2))]),
    hasAny(materialize([tuple('a\0', toFixedString('b', 1))]), [tuple('a', toFixedString('b', 2))]);
select 'mixed tuple hasAll',
    hasAll([tuple('a\0', toFixedString('b', 1))], [tuple('a', toFixedString('b', 2))]),
    hasAll(materialize([tuple('a\0', toFixedString('b', 1))]), [tuple('a', toFixedString('b', 2))]);
select 'mixed tuple indexOf',
    indexOf([tuple('a\0', toFixedString('b', 1))], tuple('a', toFixedString('b', 2))),
    indexOf(materialize([tuple('a\0', toFixedString('b', 1))]), tuple('a', toFixedString('b', 2)));
select 'mixed tuple countEqual',
    countEqual([tuple('a\0', toFixedString('b', 1))], tuple('a', toFixedString('b', 2))),
    countEqual(materialize([tuple('a\0', toFixedString('b', 1))]), tuple('a', toFixedString('b', 2)));

-- `Array` and `Map` elements. `equals` compares those through a cast to their common type, which
-- removes the padding of only the operand it converts, so the rule does not reach their elements.
-- A `Field` cannot express that, having no `FixedString` type, so the constant array has to be
-- answered by the same cast rather than by comparing `Field`s.
select 'array elem, str needle',
    has([[toFixedString('V0', 3)]], ['V0\0']),
    has(materialize([[toFixedString('V0', 3)]]), ['V0\0']);
select 'array elem, wider fs needle',
    has([[toFixedString('V0', 3)]], [toFixedString('V0', 4)]),
    has(materialize([[toFixedString('V0', 3)]]), [toFixedString('V0', 4)]);
select 'str array elem, fs needle',
    has([['V0\0']], [toFixedString('V0', 3)]),
    has(materialize([['V0\0']]), [toFixedString('V0', 3)]);
select 'map elem',
    has([map('k', toFixedString('V0', 3))], map('k', 'V0\0')),
    has(materialize([map('k', toFixedString('V0', 3))]), map('k', 'V0\0'));
select 'twice nested array elem',
    has([[[toFixedString('V0', 3)]]], [['V0\0']]),
    has(materialize([[[toFixedString('V0', 3)]]]), [['V0\0']]);
-- A container reached through a `Tuple` follows the container rule, not the tuple one.
select 'array inside tuple',
    has([tuple([toFixedString('V0', 3)])], tuple(['V0\0'])),
    has(materialize([tuple([toFixedString('V0', 3)])]), tuple(['V0\0']));
select 'array inside tuple, wider fs needle',
    has([tuple([toFixedString('V0', 3)])], tuple([toFixedString('V0', 4)])),
    has(materialize([tuple([toFixedString('V0', 3)])]), tuple([toFixedString('V0', 4)]));
select 'map inside tuple',
    has([tuple(map('k', toFixedString('V0', 3)))], tuple(map('k', 'V0\0'))),
    has(materialize([tuple(map('k', toFixedString('V0', 3)))]), tuple(map('k', 'V0\0')));
-- indexOf and countEqual take the same path.
select 'array elem indexOf',
    indexOf([[toFixedString('V0', 3)]], ['V0\0']),
    indexOf(materialize([[toFixedString('V0', 3)]]), ['V0\0']);
select 'array elem countEqual',
    countEqual([[toFixedString('V0', 3)]], ['V0\0']),
    countEqual(materialize([[toFixedString('V0', 3)]]), ['V0\0']);
-- A needle that varies per row takes the other branch of the constant-array path, and the constant
-- array is materialized for it. Each row answers 1 by agreeing with `equals` on the same operands.
select 'array elem, per-row needle', needle,
    has([[toFixedString('V0', 3)]], [needle]) = ([toFixedString('V0', 3)] = [needle])
from (select arrayJoin(['V0\0', 'V0', 'X']) as needle) order by needle;
-- The functions with their own canonicalisation, and the sorted search, over the same element.
select 'array elem hasAny',
    hasAny([[toFixedString('V0', 3)]], [['V0\0']]),
    hasAny(materialize([[toFixedString('V0', 3)]]), [['V0\0']]);
select 'array elem hasAll',
    hasAll([[toFixedString('V0', 3)]], [['V0\0']]),
    hasAll(materialize([[toFixedString('V0', 3)]]), [['V0\0']]);
select 'array elem hasSubstr',
    hasSubstr([[toFixedString('V0', 3)]], [['V0\0']]),
    hasSubstr(materialize([[toFixedString('V0', 3)]]), [['V0\0']]);
select 'array elem hasAny wider fs needle',
    hasAny([[toFixedString('V0', 3)]], [[toFixedString('V0', 4)]]),
    hasAny(materialize([[toFixedString('V0', 3)]]), [[toFixedString('V0', 4)]]);
select 'array elem indexOfAssumeSorted',
    indexOfAssumeSorted([[toFixedString('V0', 3)]], ['V0\0']),
    indexOfAssumeSorted(materialize([[toFixedString('V0', 3)]]), ['V0\0']);
select 'array elem indexOfAssumeSorted wider fs needle',
    indexOfAssumeSorted([[toFixedString('V0', 3)]], [toFixedString('V0', 4)]),
    indexOfAssumeSorted(materialize([[toFixedString('V0', 3)]]), [toFixedString('V0', 4)]);

-- `Array(LowCardinality(T))` is searched by comparing dictionary indices, and one value under the
-- rule spans several dictionary entries, so that path declines to the one comparing values. Every
-- stored spelling of the key has to be found.
select 'lc array elem, padded spelling',
    has(cast(['K1\0'], 'Array(LowCardinality(String))'), toFixedString('K1', 3)),
    has(materialize(cast(['K1\0'], 'Array(LowCardinality(String))')), toFixedString('K1', 3));
select 'lc array elem, plain spelling',
    has(cast(['K1'], 'Array(LowCardinality(String))'), toFixedString('K1', 3)),
    has(materialize(cast(['K1'], 'Array(LowCardinality(String))')), toFixedString('K1', 3));
select 'lc array elem indexOf',
    indexOf(cast(['K1\0'], 'Array(LowCardinality(String))'), toFixedString('K1', 3)),
    indexOf(materialize(cast(['K1\0'], 'Array(LowCardinality(String))')), toFixedString('K1', 3));
select 'lc array elem countEqual',
    countEqual(cast(['K1\0'], 'Array(LowCardinality(String))'), toFixedString('K1', 3)),
    countEqual(materialize(cast(['K1\0'], 'Array(LowCardinality(String))')), toFixedString('K1', 3));
-- A `LowCardinality(FixedString(N))` dictionary holds one spelling per value, so it keeps the
-- dictionary comparison, and an over-wide constant still raises rather than being answered.
select has(cast([toFixedString('K1', 3)], 'Array(LowCardinality(FixedString(3)))'), toFixedString('K1', 4)); -- { serverError TOO_LARGE_STRING_SIZE }
-- An unpadded needle is unaffected and keeps the dictionary comparison.
select 'lc array elem, unpadded needle',
    has(cast(['K1'], 'Array(LowCardinality(String))'), 'K1'),
    has(materialize(cast(['K1'], 'Array(LowCardinality(String))')), 'K1');

-- `hasAny`, `hasAll` and `hasSubstr` share one comparison with `has` and must not disagree with it
-- on the same operands. Every array argument here is a one-element array, so all four reduce to the
-- same question. Both a constant and a materialized haystack, since that is the split the issue is
-- about, and neither is otherwise covered for these three functions.
select 'hasAny hasAll hasSubstr';
select 'fs4 needle, const',
    hasAny([toFixedString('V0', 3)], [toFixedString('V0', 4)]),
    hasAll([toFixedString('V0', 3)], [toFixedString('V0', 4)]),
    hasSubstr([toFixedString('V0', 3)], [toFixedString('V0', 4)]);
select 'fs4 needle, materialized',
    hasAny(materialize([toFixedString('V0', 3)]), [toFixedString('V0', 4)]),
    hasAll(materialize([toFixedString('V0', 3)]), [toFixedString('V0', 4)]),
    hasSubstr(materialize([toFixedString('V0', 3)]), [toFixedString('V0', 4)]);
select 'str_padded needle, const',
    hasAny([toFixedString('V0', 3)], ['V0\0']),
    hasAll([toFixedString('V0', 3)], ['V0\0']),
    hasSubstr([toFixedString('V0', 3)], ['V0\0']);
select 'str_padded needle, materialized',
    hasAny(materialize([toFixedString('V0', 3)]), ['V0\0']),
    hasAll(materialize([toFixedString('V0', 3)]), ['V0\0']),
    hasSubstr(materialize([toFixedString('V0', 3)]), ['V0\0']);
-- Negative control: plain `String` against plain `String` is length-sensitive for these too.
select 'str vs str_padded must differ, hasAny',
    hasAny(['V0'], ['V0\0']),
    hasAny(materialize(['V0']), ['V0\0']);

-- Every search function must equal `arrayExists` over the same operands, which is what the issue
-- asks for. Comparing against `arrayExists` rather than a literal means no expected value is baked
-- in: if the padding rule itself is ever redefined, these rows track it instead of going stale.
select 'agreement with arrayExists';
select 'has, const',        has([toFixedString('V0', 3)], toFixedString('V0', 4))
    = arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]);
select 'has, materialized',  has(materialize([toFixedString('V0', 3)]), 'V0\0')
    = arrayExists(x -> x = 'V0\0', materialize([toFixedString('V0', 3)]));
select 'has, str elem',      has(['ab'], toFixedString('ab', 3))
    = arrayExists(x -> x = toFixedString('ab', 3), ['ab']);
select 'has, tuple elem',    has([tuple(toFixedString('V0', 3))], tuple('V0\0'))
    = arrayExists(x -> x = tuple('V0\0'), [tuple(toFixedString('V0', 3))]);
-- For the mixed tuple, compare against `equals` on the same two tuples directly: it is the rule
-- these functions have to match, and unlike `arrayExists` it cannot be rewritten into `has`.
select 'has, mixed tuple',  has([tuple('a\0', toFixedString('b', 1))], tuple('a', toFixedString('b', 2)))
    = (tuple('a\0', toFixedString('b', 1)) = tuple('a', toFixedString('b', 2)));
select 'has, mixed tuple materialized',
    has(materialize([tuple('a\0', toFixedString('b', 1))]), tuple('a', toFixedString('b', 2)))
    = (tuple('a\0', toFixedString('b', 1)) = tuple('a', toFixedString('b', 2)));
select 'hasAny, mixed tuple', hasAny([tuple('a\0', toFixedString('b', 1))], [tuple('a', toFixedString('b', 2))])
    = (tuple('a\0', toFixedString('b', 1)) = tuple('a', toFixedString('b', 2)));
select 'has, array elem',   has([[toFixedString('V0', 3)]], ['V0\0'])
    = ([toFixedString('V0', 3)] = ['V0\0']);
select 'has, array elem materialized',
    has(materialize([[toFixedString('V0', 3)]]), ['V0\0']) = ([toFixedString('V0', 3)] = ['V0\0']);
select 'has, array elem wider fs needle', has([[toFixedString('V0', 3)]], [toFixedString('V0', 4)])
    = ([toFixedString('V0', 3)] = [toFixedString('V0', 4)]);
select 'has, map elem',     has([map('k', toFixedString('V0', 3))], map('k', 'V0\0'))
    = (map('k', toFixedString('V0', 3)) = map('k', 'V0\0'));
select 'has, lc array elem',  has(cast(['K1\0'], 'Array(LowCardinality(String))'), toFixedString('K1', 3))
    = ('K1\0' = toFixedString('K1', 3));
select 'has, lc array elem materialized',
    has(materialize(cast(['K1\0'], 'Array(LowCardinality(String))')), toFixedString('K1', 3))
    = ('K1\0' = toFixedString('K1', 3));
select 'has, nullable elem',
    has(cast([toFixedString('V0', 3), null], 'Array(Nullable(FixedString(3)))'), toFixedString('V0', 4))
    = arrayExists(x -> x = toFixedString('V0', 4), cast([toFixedString('V0', 3), null], 'Array(Nullable(FixedString(3)))'));
select 'indexOf, const',     (indexOf([toFixedString('V0', 3)], toFixedString('V0', 4)) > 0)
    = arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]);
select 'countEqual, const',  (countEqual([toFixedString('V0', 3)], toFixedString('V0', 4)) > 0)
    = arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]);
select 'indexOfAssumeSorted, const',
    (indexOfAssumeSorted([toFixedString('V0', 3)], toFixedString('V0', 4)) > 0)
    = arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]);
select 'hasAny, const',      hasAny([toFixedString('V0', 3)], [toFixedString('V0', 4)])
    = arrayExists(x -> x = toFixedString('V0', 4), [toFixedString('V0', 3)]);
select 'hasAny, materialized', hasAny(materialize([toFixedString('V0', 3)]), ['V0\0'])
    = arrayExists(x -> x = 'V0\0', materialize([toFixedString('V0', 3)]));
select 'has over a map',     has(map(toFixedString('V0', 3), 1), toFixedString('V0', 4))
    = arrayExists(x -> x = toFixedString('V0', 4), mapKeys(map(toFixedString('V0', 3), 1)));
