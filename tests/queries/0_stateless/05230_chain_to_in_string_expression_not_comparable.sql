-- Whether such a query is accepted must not depend on how many comparisons are conjoined, nor on
-- whether an optimization removed one of them.
--
-- The optimizations measured here are switched by the four settings below and the first of them is
-- randomized by the test runner, so pin all four: an unpinned run would measure a different
-- optimization set on every job and the rows would stop meaning what they say.
SET optimize_and_compare_chain = 1;
SET optimize_redundant_comparisons = 1;
SET optimize_min_inequality_conjunction_chain_length = 3;
SET optimize_min_equality_disjunction_chain_length = 3;

SELECT 'ground truth: a single such comparison is refused';
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = 1; -- { serverError NO_COMMON_TYPE }

SELECT 'a conjunction chain long enough to be merged into NOT IN';
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != 1 AND s != 2 AND s != 3; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toFixedString('1', 1)) AS s) WHERE s != 1 AND s != 2 AND s != 3; -- { serverError NO_COMMON_TYPE }
-- A LowCardinality expression is merged without reaching the length threshold, so two terms are
-- enough here and raising the threshold does not put the chain back.
SELECT count() FROM (SELECT materialize(toLowCardinality('1')) AS s) WHERE s != 1 AND s != 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toLowCardinality('1')) AS s) WHERE s != 1 AND s != 2
SETTINGS optimize_min_inequality_conjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }

SELECT 'a disjunction chain long enough to be merged into IN';
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = 1 OR s = 2 OR s = 3; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toFixedString('1', 1)) AS s) WHERE s = 1 OR s = 2 OR s = 3; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toNullable('1')) AS s) WHERE s = 1 OR s = 2 OR s = 3; -- { serverError NO_COMMON_TYPE }

SELECT 'a redundant comparison pruned away by a stronger one on the same expression';
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s > 'x' AND s > 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = 'x' AND s != 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = '1' AND s = 1; -- { serverError NO_COMMON_TYPE }

SELECT 'a conjunction collapsed to false by a conflict';
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = 1 AND s = 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = 1 AND s != 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s > 1 AND s <= 1; -- { serverError NO_COMMON_TYPE }
-- The conflict is on a different expression, so the collapse would drop the ill-typed operand
-- without ever looking at it.
SELECT count() FROM (SELECT materialize('1') AS s, materialize(1) AS i) WHERE i = 1 AND i = 2 AND s != 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s, materialize(1) AS i) WHERE i = 1 AND i = 2 AND s = 1; -- { serverError NO_COMMON_TYPE }

SELECT 'an equality inferred through a transitive chain';
SELECT count() FROM (SELECT materialize('1') AS s, materialize('2') AS t) WHERE s = t AND t = 1 AND t = 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s, materialize('2') AS t) WHERE s = t AND t != 1 AND t != 2 AND t != 3; -- { serverError NO_COMMON_TYPE }

SELECT 'shapes that keep one such comparison whatever the optimizations do';
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != 1 AND s != 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s > 1 AND s > 2; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s >= 1 AND s <= 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != 1 AND s != 1; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s < 'z' AND s < 1; -- { serverError NO_COMMON_TYPE }

-- A conflict between two comparisons that are themselves executable still reports the conflicting
-- operand's own error, which is not NO_COMMON_TYPE. This is the pre-existing veto's own class and it
-- must keep behaving as before.
SELECT count() FROM (SELECT materialize('1') AS s, materialize(1) AS i) WHERE i = 1 AND i = 2 AND i > 'str'; -- { serverError TYPE_MISMATCH }

SELECT 'a tuple with one such position, compared element by element';
-- Two tuples of equal size are admitted whatever their elements are, and are then compared element by
-- element, so one such position refuses the whole comparison. These two rows are the ground truth: a
-- single comparison throws, before any chain is long enough to be merged.
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t = tuple(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t != tuple(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t = tuple(1) OR t = tuple(2) OR t = tuple(3); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t != tuple(1) AND t != tuple(2) AND t != tuple(3); -- { serverError NO_COMMON_TYPE }
-- Raising the threshold leaves the chain unmerged, which is the state whose result used to differ.
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t != tuple(1) AND t != tuple(2) AND t != tuple(3)
SETTINGS optimize_min_inequality_conjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t = tuple(1) OR t = tuple(2) OR t = tuple(3)
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }
-- One refusing position is enough when the tuple's other position is comparable.
SELECT count() FROM (SELECT (materialize('1'), materialize(2)) AS t) WHERE t = (1, 1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT (materialize('1'), materialize(2)) AS t) WHERE t = (1, 1) OR t = (2, 2) OR t = (3, 3); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT (materialize('1'), materialize(2)) AS t) WHERE t != (1, 1) AND t != (2, 2) AND t != (3, 3); -- { serverError NO_COMMON_TYPE }
-- The position may be nested any number of levels down.
SELECT count() FROM (SELECT tuple(tuple(materialize('1'))) AS t) WHERE t = tuple(tuple(1)); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(tuple(materialize('1'))) AS t) WHERE t = tuple(tuple(1)) OR t = tuple(tuple(2)) OR t = tuple(tuple(3)); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(tuple(materialize('1'))) AS t) WHERE t != tuple(tuple(1)) AND t != tuple(tuple(2)) AND t != tuple(tuple(3)); -- { serverError NO_COMMON_TYPE }
-- An element carries its own LowCardinality and Nullable wrappers, which have to be stripped at the
-- level they sit at rather than only at the top. A Tuple(Nullable(String)) conjunction chain is never
-- merged, so for that element type it is the disjunction that used to be accepted.
SELECT count() FROM (SELECT tuple(materialize(toNullable('1'))) AS t) WHERE t = tuple(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize(toNullable('1'))) AS t) WHERE t != tuple(1) AND t != tuple(2) AND t != tuple(3); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize(toNullable('1'))) AS t) WHERE t = tuple(1) OR t = tuple(2) OR t = tuple(3); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize(toLowCardinality('1'))) AS t) WHERE t != tuple(1) AND t != tuple(2) AND t != tuple(3); -- { serverError NO_COMMON_TYPE }
-- The pruning and the conflict paths reach a tuple expression as well.
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t = tuple('1') AND t = tuple(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t = tuple(1) AND t = tuple(2); -- { serverError NO_COMMON_TYPE }

SELECT 'a constant whose common type with the string is not itself a string';
-- Only the disjunction was merged for such a constant; the conjunction's notIn is declined for an
-- unrelated reason further down.
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = CAST(1, 'Dynamic'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != CAST(1, 'Dynamic'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s)
    WHERE s = CAST(1, 'Dynamic') OR s = CAST(2, 'Dynamic') OR s = CAST(3, 'Dynamic'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s)
    WHERE s != CAST(1, 'Dynamic') AND s != CAST(2, 'Dynamic') AND s != CAST(3, 'Dynamic'); -- { serverError NO_COMMON_TYPE }
-- A Variant constant reaches the same positions, one of them inside a tuple.
SELECT count() FROM (SELECT materialize('1') AS s) WHERE s = CAST(1, 'Variant(UInt8)'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize('1') AS s)
    WHERE s = CAST(1, 'Variant(UInt8)') OR s = CAST(2, 'Variant(UInt8)') OR s = CAST(3, 'Variant(UInt8)'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t)
    WHERE t = tuple(CAST(1, 'Variant(UInt8)')) OR t = tuple(CAST(2, 'Variant(UInt8)'))
       OR t = tuple(CAST(3, 'Variant(UInt8)')); -- { serverError NO_COMMON_TYPE }
-- A control, not a defect row: a Dynamic element inside a tuple is refused at every chain length
-- whatever this pass does. Pinned so that a change making it reachable is caught here.
SELECT count() FROM (SELECT tuple(materialize('1')) AS t) WHERE t = tuple(CAST(1, 'Dynamic')); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize('1')) AS t)
    WHERE t = tuple(CAST(1, 'Dynamic')) OR t = tuple(CAST(2, 'Dynamic'))
       OR t = tuple(CAST(3, 'Dynamic')); -- { serverError NO_COMMON_TYPE }

SELECT 'a constant that only carries its value, against an expression outside the string family';
-- The expression is not in the string family, so the rules above never look at the constant. A carried
-- string is nevertheless not coerced the way a plain constant is, because the adaptor that dispatches on
-- the carrier materializes it first. The first row is the ground truth: one such comparison throws.
SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u) WHERE u = CAST('1', 'Dynamic'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Dynamic') OR u = CAST('2', 'Dynamic') OR u = CAST('3', 'Dynamic'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Dynamic') OR u = CAST('2', 'Dynamic') OR u = CAST('3', 'Dynamic')
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }
-- A fixed alternative set reports no dynamic structure, so a Variant carrier has to be asked for by name.
SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u) WHERE u = CAST('1', 'Variant(String)'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Variant(String)') OR u = CAST('2', 'Variant(String)')
       OR u = CAST('3', 'Variant(String)'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Variant(String)') OR u = CAST('2', 'Variant(String)')
       OR u = CAST('3', 'Variant(String)')
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }
-- A carrier one tuple position down. Neither Tuple nor Variant reports dynamic structure for a fixed
-- alternative set, so this shape is reached only by the element-wise recursion arriving at the carrier
-- test, and it is the row that pins the two composing.
SELECT count() FROM (SELECT tuple(materialize(toUInt8(1))) AS t)
    WHERE t = tuple(CAST('1', 'Variant(String)')); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize(toUInt8(1))) AS t)
    WHERE t = tuple(CAST('1', 'Variant(String)')) OR t = tuple(CAST('2', 'Variant(String)'))
       OR t = tuple(CAST('3', 'Variant(String)')); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(materialize(toUInt8(1))) AS t)
    WHERE t = tuple(CAST('1', 'Variant(String)')) OR t = tuple(CAST('2', 'Variant(String)'))
       OR t = tuple(CAST('3', 'Variant(String)'))
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }

SELECT 'a Variant expression with an alternative the constant cannot be compared with';
-- Each row is dispatched on the alternative it holds, which refuses a String alternative against a
-- UInt8 constant. The first row is the ground truth: a single such comparison throws.
SELECT count() FROM (SELECT CAST(materialize('x'), 'Variant(String, UInt8)') AS v)
    WHERE v = toUInt8(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT CAST(materialize('x'), 'Variant(String, UInt8)') AS v)
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3); -- { serverError NO_COMMON_TYPE }
-- Raising the threshold leaves the chain unmerged, which is the state whose result used to differ.
SELECT count() FROM (SELECT CAST(materialize('x'), 'Variant(String, UInt8)') AS v)
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3)
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }
-- The same alternative one level down, where the element-wise recursion meets the alternative-wise one.
SELECT count() FROM (SELECT tuple(CAST(materialize('x'), 'Variant(String, UInt8)')) AS t)
    WHERE t = tuple(toUInt8(1)); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(CAST(materialize('x'), 'Variant(String, UInt8)')) AS t)
    WHERE t = tuple(toUInt8(1)) OR t = tuple(toUInt8(2)) OR t = tuple(toUInt8(3)); -- { serverError NO_COMMON_TYPE }
-- A constant of the expression's own declared type is refused for the same reason, and only because the
-- executability test runs before an equal declared type is returned unchanged: these rows pin that order.
SELECT count() FROM (SELECT CAST(materialize('x'), 'Variant(String, UInt8)') AS v)
    WHERE v = CAST(1, 'Variant(String, UInt8)'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT CAST(materialize('x'), 'Variant(String, UInt8)') AS v)
    WHERE v = CAST(1, 'Variant(String, UInt8)') OR v = CAST(2, 'Variant(String, UInt8)')
       OR v = CAST(3, 'Variant(String, UInt8)'); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(CAST(materialize('x'), 'Variant(String, UInt8)')) AS t)
    WHERE t = tuple(CAST(1, 'Variant(String, UInt8)')); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT tuple(CAST(materialize('x'), 'Variant(String, UInt8)')) AS t)
    WHERE t = tuple(CAST(1, 'Variant(String, UInt8)')) OR t = tuple(CAST(2, 'Variant(String, UInt8)'))
       OR t = tuple(CAST(3, 'Variant(String, UInt8)')); -- { serverError NO_COMMON_TYPE }
-- An alternative can also be refused outside the string family, with the comparison's own error rather
-- than NO_COMMON_TYPE. The first row is again the ground truth: one such comparison throws.
SELECT count() FROM (SELECT CAST(reinterpretAsUUID(1), 'Variant(UInt8, UUID)') AS v)
    WHERE v = toUInt8(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT CAST(reinterpretAsUUID(1), 'Variant(UInt8, UUID)') AS v)
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT CAST(reinterpretAsUUID(1), 'Variant(UInt8, UUID)') AS v)
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3)
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT tuple(CAST(reinterpretAsUUID(1), 'Variant(UInt8, UUID)')) AS t)
    WHERE t = tuple(toUInt8(1)) OR t = tuple(toUInt8(2))
       OR t = tuple(toUInt8(3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- The conjunction form of the same shape is not merged at this length, so it throws for the same reason
-- a single comparison does. Pinned so that a change making it reachable is caught here.
SELECT count() FROM (SELECT CAST(reinterpretAsUUID(1), 'Variant(UInt8, UUID)') AS v)
    WHERE v != toUInt8(1) AND v != toUInt8(2) AND v != toUInt8(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- comparisons that ARE executable must keep being merged ---';

-- String against String, Enum and FixedString constants: all three have String as a common type, so
-- the comparison executes and the merge is correct. The Enum row is the one that a guard keyed on the
-- constant's type name rather than on the absence of a common type would wrongly reject.
SELECT 'string_vs_string', count() FROM (SELECT materialize('1') AS s) WHERE s != 'a' AND s != 'b' AND s != 'c';
SELECT 'string_vs_enum', count() FROM (SELECT materialize('a') AS s)
    WHERE s = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')
       OR s = CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')
       OR s = CAST('c', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)');
SELECT 'string_vs_fixed_string', count() FROM (SELECT materialize('ab') AS s)
    WHERE s != toFixedString('ab', 2) AND s != toFixedString('cd', 2) AND s != toFixedString('ef', 2);

-- Two pairs that have no common type and are nevertheless executable, so they must still be merged:
-- an array pair compares element-wise, and a UUID expression against a constant String is coerced
-- because the constant is the string side.
SELECT 'array_vs_array', count() FROM (SELECT materialize([toInt64(1)]) AS a)
    WHERE a != [toUInt64(1)] AND a != [toUInt64(2)] AND a != [toUInt64(3)];
-- The first excluded value is the expression's own, so this row measures the coercion rather than only
-- that the query ran: it answers 0 where excluding three absent values would answer 1 either way.
SELECT 'uuid_vs_string_constants', count() FROM (SELECT materialize(reinterpretAsUUID(1)) AS c)
    WHERE c != '00000000-0000-0001-0000-000000000000'
      AND c != '00000000-0000-0000-0000-000000000002'
      AND c != '00000000-0000-0000-0000-000000000003';

-- The same two pairs inside a tuple: the element-wise test has to keep them, so it may not be spelled
-- as "no common type anywhere" but only as "a string-family position with no common type". The first
-- excluded UUID is the expression's own value here too, so the row answers 0 rather than 1 whichever way
-- the chain is optimized. This merge fires on neither arm, so it measures the coercion and not the fold.
SELECT 'tuple_array_vs_array', count() FROM (SELECT tuple(materialize([toInt64(1)])) AS t)
    WHERE t != tuple([toUInt64(1)]) AND t != tuple([toUInt64(2)]) AND t != tuple([toUInt64(3)]);
SELECT 'tuple_uuid_vs_string_constants', count() FROM (SELECT tuple(materialize(reinterpretAsUUID(1))) AS t)
    WHERE t != tuple('00000000-0000-0001-0000-000000000000')
      AND t != tuple('00000000-0000-0000-0000-000000000002')
      AND t != tuple('00000000-0000-0000-0000-000000000003');

-- A Dynamic or Variant constant holding a string IS comparable with a string expression, so these
-- chains must keep answering. Their common type is Dynamic or absent, so the merge is declined either
-- way, which costs the fold and not the answer: hence a result assertion rather than a merge one.
SELECT 'dynamic_string_constant', count() FROM (SELECT materialize('1') AS s)
    WHERE s = CAST('1', 'Dynamic') OR s = CAST('2', 'Dynamic') OR s = CAST('3', 'Dynamic');
SELECT 'variant_string_constant', count() FROM (SELECT materialize('1') AS s)
    WHERE s = CAST('1', 'Variant(String)') OR s = CAST('2', 'Variant(String)')
       OR s = CAST('3', 'Variant(String)');

-- Three result rows bounding the alternative-wise test: no string-family alternative (merge declined,
-- answer still 3), a String constant against a UInt8 alternative (merge kept, so the test may not be a
-- blanket Variant decline), and all alternatives comparable. All three select three of ten rows, so a
-- filter that was silently dropped would answer 10 rather than 3.
SELECT 'variant_no_string_alternative', count() FROM (
    SELECT CAST(toUInt8(number), 'Variant(UInt8, UUID)') AS v FROM numbers(10))
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3);
SELECT 'variant_string_constant_expression', count() FROM (
    SELECT CAST(arrayJoin(['aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'jj']),
        'Variant(String, UInt8)') AS v)
    WHERE v = 'bb' OR v = 'cc' OR v = 'dd';
SELECT 'variant_single_alternative', count() FROM (
    SELECT CAST(toUInt8(number), 'Variant(UInt8)') AS v FROM numbers(10))
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3);

-- These assert that the merge does happen and that raising the length threshold is what stops it, so a
-- change that silently declined it would be caught here rather than passing unnoticed.
SELECT 'array_merged', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize([toInt64(1)]) AS a)
    WHERE a != [toUInt64(1)] AND a != [toUInt64(2)] AND a != [toUInt64(3)]);
SELECT 'array_not_merged_above_threshold', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize([toInt64(1)]) AS a)
    WHERE a != [toUInt64(1)] AND a != [toUInt64(2)] AND a != [toUInt64(3)])
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

SELECT 'tuple_array_merged', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT tuple(materialize([toInt64(1)])) AS t)
    WHERE t != tuple([toUInt64(1)]) AND t != tuple([toUInt64(2)]) AND t != tuple([toUInt64(3)]));
SELECT 'tuple_array_not_merged_above_threshold', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT tuple(materialize([toInt64(1)])) AS t)
    WHERE t != tuple([toUInt64(1)]) AND t != tuple([toUInt64(2)]) AND t != tuple([toUInt64(3)]))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

SELECT 'uuid_merged', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize(reinterpretAsUUID(1)) AS c)
    WHERE c != '00000000-0000-0001-0000-000000000000'
      AND c != '00000000-0000-0000-0000-000000000002'
      AND c != '00000000-0000-0000-0000-000000000003');
SELECT 'uuid_not_merged_above_threshold', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize(reinterpretAsUUID(1)) AS c)
    WHERE c != '00000000-0000-0001-0000-000000000000'
      AND c != '00000000-0000-0000-0000-000000000002'
      AND c != '00000000-0000-0000-0000-000000000003')
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

SELECT 'string_vs_string_merged', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != 'a' AND s != 'b' AND s != 'c');
SELECT 'string_vs_string_not_merged_above_threshold', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize('1') AS s) WHERE s != 'a' AND s != 'b' AND s != 'c')
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

SELECT 'string_vs_enum_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize('a') AS s)
    WHERE s = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')
       OR s = CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')
       OR s = CAST('c', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'));
SELECT 'string_vs_enum_not_merged_above_threshold', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize('a') AS s)
    WHERE s = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')
       OR s = CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')
       OR s = CAST('c', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'numeric_merged', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u != toUInt16(1) AND u != toUInt16(2) AND u != toUInt16(3));
SELECT 'numeric_not_merged_above_threshold', countIf(explain LIKE '%function_name: notIn%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u != toUInt16(1) AND u != toUInt16(2) AND u != toUInt16(3))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

SELECT 'variant_no_string_alternative_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toUInt8(number), 'Variant(UInt8, UUID)') AS v FROM numbers(10))
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3));
SELECT 'variant_no_string_alternative_not_merged_above_threshold', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toUInt8(number), 'Variant(UInt8, UUID)') AS v FROM numbers(10))
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'variant_string_constant_expression_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(arrayJoin(['aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'jj']),
        'Variant(String, UInt8)') AS v)
    WHERE v = 'bb' OR v = 'cc' OR v = 'dd');
SELECT 'variant_string_constant_expression_not_merged_above_threshold', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(arrayJoin(['aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg', 'hh', 'ii', 'jj']),
        'Variant(String, UInt8)') AS v)
    WHERE v = 'bb' OR v = 'cc' OR v = 'dd')
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'variant_single_alternative_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toUInt8(number), 'Variant(UInt8)') AS v FROM numbers(10))
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3));
SELECT 'variant_single_alternative_not_merged_above_threshold', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toUInt8(number), 'Variant(UInt8)') AS v FROM numbers(10))
    WHERE v = toUInt8(1) OR v = toUInt8(2) OR v = toUInt8(3))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

-- Made permissive, the mismatch answers instead of throwing, and then the merge changed the ANSWER and
-- not merely whether the query was accepted: the carried strings convert losslessly to the expression's
-- own type, so the merged chain excluded a value the comparison it replaced never excluded. One term and
-- three terms must now agree. This is the only row here that would catch a wrong answer rather than a
-- wrongly accepted query, so it is the one that may not be dropped.
SELECT 'carrier_permissive_one_term', count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Dynamic')
SETTINGS dynamic_throw_on_type_mismatch = 0;
SELECT 'carrier_permissive_three_terms', count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Dynamic') OR u = CAST('2', 'Dynamic') OR u = CAST('3', 'Dynamic')
SETTINGS dynamic_throw_on_type_mismatch = 0;

-- The folds the two new arms decline. Each asserts 0 against `numeric_merged` above, which is the same
-- chain shape on a plain numeric expression and asserts 1, so a 0 here cannot come from the merge having
-- stopped working; and against the threshold row, which is 0 on both arms because nothing is merged there.
SELECT 'carrier_dynamic_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Dynamic') OR u = CAST('2', 'Dynamic') OR u = CAST('3', 'Dynamic'));
SELECT 'carrier_variant_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT materialize(toUInt8(1)) AS u)
    WHERE u = CAST('1', 'Variant(String)') OR u = CAST('2', 'Variant(String)')
       OR u = CAST('3', 'Variant(String)'));
SELECT 'array_tuple_variant_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT [tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))] AS a)
    WHERE a = [tuple(toUInt8(1))] OR a = [tuple(toUInt8(2))] OR a = [tuple(toUInt8(3))]);
SELECT 'array_tuple_variant_not_merged_above_threshold', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT [tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))] AS a)
    WHERE a = [tuple(toUInt8(1))] OR a = [tuple(toUInt8(2))] OR a = [tuple(toUInt8(3))])
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'array2_tuple_variant_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT [[tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))]] AS a)
    WHERE a = [[tuple(toUInt8(1))]] OR a = [[tuple(toUInt8(2))]] OR a = [[tuple(toUInt8(3))]]);
SELECT 'tuple_carrier_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT tuple(materialize(toUInt8(1))) AS t)
    WHERE t = tuple(CAST('1', 'Variant(String)')) OR t = tuple(CAST('2', 'Variant(String)'))
       OR t = tuple(CAST('3', 'Variant(String)')));

-- The alternative-wise test's second clause declines an alternative with no common type with the
-- constant, which is wider than executability: Int64 against a UInt64 constant has no common type and
-- compares perfectly well. The cost is the fold and never the answer, and these three rows pin exactly
-- that: the same chain on a plain Int64 expression is still merged, and both answer 3.
SELECT 'variant_cross_numeric', count() FROM (SELECT CAST(toInt64(number), 'Variant(Int64)') AS v FROM numbers(10))
    WHERE v = toUInt64(1) OR v = toUInt64(2) OR v = toUInt64(3);
SELECT 'variant_cross_numeric_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toInt64(number), 'Variant(Int64)') AS v FROM numbers(10))
    WHERE v = toUInt64(1) OR v = toUInt64(2) OR v = toUInt64(3));
SELECT 'plain_cross_numeric_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT toInt64(number) AS v FROM numbers(10))
    WHERE v = toUInt64(1) OR v = toUInt64(2) OR v = toUInt64(3));

-- A FixedString(16) is comparable with an IPv6 even though the two have no common type, so this pair
-- must not be declined for that reason. The merge does not happen here for an unrelated reason (the
-- IPv6 constant has no lossless FixedString(16) form), which is why the assertions are on results.
DROP TABLE IF EXISTS t_fixed_string_16;
CREATE TABLE t_fixed_string_16 (a FixedString(16)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fixed_string_16 VALUES (unhex('00000000000000000000000000000001'));
SELECT 'fixed_string_16_vs_ipv6_one_term', count() FROM t_fixed_string_16 WHERE a != toIPv6('::1');
SELECT 'fixed_string_16_vs_ipv6_three_terms', count() FROM t_fixed_string_16
    WHERE a != toIPv6('::1') AND a != toIPv6('::2') AND a != toIPv6('::3');
SELECT 'fixed_string_16_vs_ipv6_disjunction', count() FROM t_fixed_string_16
    WHERE a = toIPv6('::1') OR a = toIPv6('::2') OR a = toIPv6('::3');
DROP TABLE t_fixed_string_16;

-- A String array element is refused when the comparison is resolved, at any chain length, so the
-- optimizations never reach it. Pinned as a control: the array recursion below must not change it.
SELECT count() FROM (SELECT materialize(['1']) AS a) WHERE a != [1]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT materialize(['1']) AS a) WHERE a != [1] AND a != [2] AND a != [3]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- A direct Variant element is refused when resolved too, on every arm, so it is not this class either.
SELECT count() FROM (SELECT [CAST(materialize('x'), 'Variant(String, UInt8)')] AS a)
    WHERE a = [toUInt8(1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT [CAST(materialize('x'), 'Variant(String, UInt8)')] AS a)
    WHERE a = [toUInt8(1)] OR a = [toUInt8(2)] OR a = [toUInt8(3)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- An element that defers its own refusal to the rows it holds is a different matter. The array's element
-- pair is resolved through the adaptor, so the pair is admitted and the refusal waits for execution; the
-- three-term form of this shape used to answer. The first row is the ground truth.
SELECT count() FROM (SELECT [tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))] AS a)
    WHERE a = [tuple(toUInt8(1))]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT [tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))] AS a)
    WHERE a = [tuple(toUInt8(1))] OR a = [tuple(toUInt8(2))]
       OR a = [tuple(toUInt8(3))]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT [tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))] AS a)
    WHERE a = [tuple(toUInt8(1))] OR a = [tuple(toUInt8(2))] OR a = [tuple(toUInt8(3))]
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Two array levels down, where the recursion has to apply twice.
SELECT count() FROM (SELECT [[tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))]] AS a)
    WHERE a = [[tuple(toUInt8(1))]]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT [[tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))]] AS a)
    WHERE a = [[tuple(toUInt8(1))]] OR a = [[tuple(toUInt8(2))]]
       OR a = [[tuple(toUInt8(3))]]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT [[tuple(CAST(materialize('x'), 'Variant(String, UInt8)'))]] AS a)
    WHERE a = [[tuple(toUInt8(1))]] OR a = [[tuple(toUInt8(2))]] OR a = [[tuple(toUInt8(3))]]
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The mirror shape (an array below a tuple), a Map pair and an AggregateFunction pair are all refused
-- when the comparison is resolved rather than at execution, so none of them was ever reachable and none
-- needs an arm. Measured, not argued, and pinned as controls so that a change admitting such a pair is
-- caught here. An AggregateFunction is not comparable for equality at all, whatever it carries.
SELECT count() FROM (SELECT tuple([CAST(materialize('x'), 'Variant(String, UInt8)')]) AS t)
    WHERE t = tuple([toUInt8(1)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT tuple([CAST(materialize('x'), 'Variant(String, UInt8)')]) AS t)
    WHERE t = tuple([toUInt8(1)]) OR t = tuple([toUInt8(2)])
       OR t = tuple([toUInt8(3)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT map('k', CAST(materialize('x'), 'Variant(String, UInt8)')) AS m)
    WHERE m = map('k', toUInt8(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT map('k', CAST(materialize('x'), 'Variant(String, UInt8)')) AS m)
    WHERE m = map('k', toUInt8(1)) OR m = map('k', toUInt8(2))
       OR m = map('k', toUInt8(3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (
    SELECT initializeAggregation('argMaxState', CAST(materialize('x'), 'Variant(String, UInt8)'), toUInt8(1)) AS g)
    WHERE g = initializeAggregation('argMaxState', CAST('y', 'Variant(String, UInt8)'),
        toUInt8(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (
    SELECT initializeAggregation('argMaxState', CAST(materialize('x'), 'Variant(String, UInt8)'), toUInt8(1)) AS g)
    WHERE g = initializeAggregation('argMaxState', CAST('y', 'Variant(String, UInt8)'), toUInt8(1))
       OR g = initializeAggregation('argMaxState', CAST('z', 'Variant(String, UInt8)'), toUInt8(1))
       OR g = initializeAggregation('argMaxState', CAST('w', 'Variant(String, UInt8)'),
        toUInt8(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'an interval carrier, where the executability probe itself used to throw';

-- `tryGetLeastSupertype` reports the absence of a common type by returning null for every pair except
-- two intervals whose groups differ, where it throws instead, and it recurses into nested types. The
-- probe used that null as a total answer, so the exception escaped the pass and refused a chain of
-- exactly the merge length while both the single comparison and the same chain above the threshold
-- answered. Each shape below is therefore measured at one term, at the threshold and above it.

-- Two clashing intervals in one Variant need the suspicious-types setting. The rows hold the alternative
-- the constant matches, so the comparison runs, and the three lengths must agree on that.
SELECT 'interval_variant_one_term', count() FROM (
    SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, IntervalMonth)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1)
SETTINGS allow_suspicious_variant_types = 1;
SELECT 'interval_variant_three_terms', count() FROM (
    SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, IntervalMonth)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1) OR v = toIntervalDay(2) OR v = toIntervalDay(3)
SETTINGS allow_suspicious_variant_types = 1;
SELECT 'interval_variant_above_threshold', count() FROM (
    SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, IntervalMonth)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1) OR v = toIntervalDay(2) OR v = toIntervalDay(3)
SETTINGS allow_suspicious_variant_types = 1, optimize_min_equality_disjunction_chain_length = 100;
SELECT 'interval_variant_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, IntervalMonth)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1) OR v = toIntervalDay(2) OR v = toIntervalDay(3))
SETTINGS allow_suspicious_variant_types = 1;

-- One interval paired with a type that has no common type with it is a Variant the suspicious-types
-- setting does not gate, because its own probe reads that null as permission, so this shape needs
-- nothing set. All three lengths refuse, which is why the merge row below is the one that moves: the
-- refusal now comes from execution rather than from the pass.
SELECT count() FROM (SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, String)') AS v FROM numbers(10))
    WHERE v = toIntervalMonth(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, String)') AS v FROM numbers(10))
    WHERE v = toIntervalMonth(1) OR v = toIntervalMonth(2)
       OR v = toIntervalMonth(3); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, String)') AS v FROM numbers(10))
    WHERE v = toIntervalMonth(1) OR v = toIntervalMonth(2) OR v = toIntervalMonth(3)
SETTINGS optimize_min_equality_disjunction_chain_length = 100; -- { serverError NO_COMMON_TYPE }
SELECT 'interval_default_settings_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay, String)') AS v FROM numbers(10))
    WHERE v = toIntervalMonth(1) OR v = toIntervalMonth(2) OR v = toIntervalMonth(3));

-- The clashing pair one level down, which the recursion inside `getLeastSupertype` reaches and a guard
-- spelled `isInterval(expression) && isInterval(constant)` would not.
SELECT count() FROM (SELECT CAST(tuple(toIntervalDay(1)), 'Variant(Tuple(IntervalDay))') AS v)
    WHERE v = tuple(toIntervalMonth(1)); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM (SELECT CAST(tuple(toIntervalDay(1)), 'Variant(Tuple(IntervalDay))') AS v)
    WHERE v = tuple(toIntervalMonth(1)) OR v = tuple(toIntervalMonth(2))
       OR v = tuple(toIntervalMonth(3)); -- { serverError NO_COMMON_TYPE }
SELECT 'interval_nested_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(tuple(toIntervalDay(1)), 'Variant(Tuple(IntervalDay))') AS v)
    WHERE v = tuple(toIntervalMonth(1)) OR v = tuple(toIntervalMonth(2)) OR v = tuple(toIntervalMonth(3)));

-- An array of intervals as the alternative is refused when the comparison is resolved, at any length, so
-- it never reaches the probe. A control: the recursion above must not start admitting it.
SELECT count() FROM (SELECT CAST([toIntervalDay(1)], 'Variant(Array(IntervalDay))') AS v)
    WHERE v = [toIntervalMonth(1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM (SELECT CAST([toIntervalDay(1)], 'Variant(Array(IntervalDay))') AS v)
    WHERE v = [toIntervalMonth(1)] OR v = [toIntervalMonth(2)]
       OR v = [toIntervalMonth(3)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The decline is keyed on an interval being present rather than on the pair that throws, so it is wider
-- than the throw it avoids, exactly as the `variant_cross_numeric` trio above pins for the sibling
-- clause. Here it costs nothing even in folds: this chain was already not merged, for the unrelated
-- reason that the constant has no lossless `Variant(IntervalDay)` form. So these rows pin the ANSWER,
-- which the decline may never change, and the merge row stays 0 on both arms.
SELECT 'interval_single_alternative_one_term', count() FROM (
    SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1);
SELECT 'interval_single_alternative_three_terms', count() FROM (
    SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1) OR v = toIntervalDay(2) OR v = toIntervalDay(3);
SELECT 'interval_single_alternative_above_threshold', count() FROM (
    SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1) OR v = toIntervalDay(2) OR v = toIntervalDay(3)
SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'interval_single_alternative_not_merged', countIf(explain LIKE '%function_name: in,%') FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM (SELECT CAST(toIntervalDay(number), 'Variant(IntervalDay)') AS v FROM numbers(10))
    WHERE v = toIntervalDay(1) OR v = toIntervalDay(2) OR v = toIntervalDay(3));

-- A projection definition is rewritten by the old AST-level optimizer rather than by this pass, and
-- keeps merging such a chain. That is deliberate: the same code path runs during ATTACH, so rejecting a
-- definition that is accepted today would leave an existing table unable to attach. Unchanged here.
DROP TABLE IF EXISTS t_projection_chain;
CREATE TABLE t_projection_chain (s String, PROJECTION p (SELECT s WHERE s = 1 OR s = 2 OR s = 3 ORDER BY s))
    ENGINE = MergeTree ORDER BY tuple();
SELECT 'projection_three_terms_accepted', count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_chain';
DROP TABLE t_projection_chain;
CREATE TABLE t_projection_chain (s String, PROJECTION p (SELECT s WHERE s = 1 OR s = 2 ORDER BY s))
    ENGINE = MergeTree ORDER BY tuple(); -- { serverError NO_COMMON_TYPE }

SELECT '--- the reported query ---';

-- A regression pin, not a defect row: the reported query is refused before and after this change,
-- because the WHERE binds c1 to the String alias in the SELECT list rather than to the UUID column.
-- It is pinned at two and at three inequalities because only the three term form is long enough to
-- be merged, and the report was that the two forms disagreed.
DROP TABLE IF EXISTS t_uuid;
CREATE TABLE t_uuid (c1 UUID) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_uuid SELECT reinterpretAsUUID(number) FROM numbers(10);
SELECT toString(c1) AS c1, count() AS n FROM t_uuid
WHERE (c1 != toUUID('00000000-0000-0000-0000-000000000001'))
  AND (c1 != toUUID('00000000-0000-0000-0000-000000000002'))
GROUP BY c1 ORDER BY n LIMIT 25; -- { serverError NO_COMMON_TYPE }
SELECT toString(c1) AS c1, count() AS n FROM t_uuid
WHERE (c1 != toUUID('00000000-0000-0000-0000-000000000001'))
  AND (c1 != toUUID('00000000-0000-0000-0000-000000000002'))
  AND (c1 != toUUID('00000000-0000-0000-0000-000000000003'))
GROUP BY c1 ORDER BY n LIMIT 25; -- { serverError NO_COMMON_TYPE }
-- The same filter against the UUID column itself is what the reporter meant. The three excluded values
-- are drawn from the table, so the row measures the filter: 7 of the 10 rows survive it. Excluding
-- values that are absent would report 10 whether the filter was applied or silently dropped.
SELECT 'uuid_column_not_in', count() FROM t_uuid WHERE c1 NOT IN (
    reinterpretAsUUID(1),
    reinterpretAsUUID(2),
    reinterpretAsUUID(3));
DROP TABLE t_uuid;
