-- negative tests
SELECT arraySymmetricDifference(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arraySymmetricDifference(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference(1, [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference([1, 2], 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arraySymmetricDifference(1, [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- const inputs
SELECT arraySymmetricDifference([]);
SELECT arraySymmetricDifference([1, 2]);
SELECT arraySymmetricDifference([1, 2], [1, 3]);
SELECT arraySymmetricDifference(['a', 'b'], ['a', 'c']);
SELECT arraySymmetricDifference([1, NULL], [1, 3]);
SELECT arraySymmetricDifference([1, NULL], [NULL, 3]);
SELECT arraySymmetricDifference([1, 2], [1, 2], [1, 2]);
SELECT arraySymmetricDifference([1, 2], [1, 2], [1, 3]);
SELECT toTypeName(arraySymmetricDifference([(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])]));

-- non-const inputs
DROP TABLE IF EXISTS tab;

CREATE TABLE tab (
    id           UInt8,
    arr_tpl_1    Array( Tuple(Nullable(Int32),Array(String)) ),
    arr_tpl_2    Array( Tuple(Nullable(Int32),Array(String)) ),
    arr_lc_str_1 Array( LowCardinality(String) ),
    arr_lc_str_2 Array( LowCardinality(String) )
) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (1, [(1, ['a', 'b']), (Null, ['c'])], [(2, ['c', Null]), (1, ['a', 'b'])], ['a', 'b', 'c'], ['c', 'a', 'b']);
INSERT INTO tab VALUES (2, [(2, ['d', 'e']), (Null, ['f'])], [(3, ['g', Null]), (2, ['d', 'e'])], ['d', 'e', 'f'], ['f', 'd', 'e']);
INSERT INTO tab VALUES (3, [(3, ['g', 'h']), (Null, ['i'])], [(4, ['j', Null]), (3, ['g', 'h'])], ['g', 'h', 'i'], ['i', 'g', 'h']);
INSERT INTO tab VALUES (4, [(4, ['j', 'k']), (Null, ['l'])], [(5, ['m', Null]), (4, ['j', 'k'])], ['j', 'k', 'l'], ['l', 'j', 'k']);

SELECT arraySymmetricDifference(arr_tpl_1, arr_tpl_2) FROM tab ORDER BY id;
SELECT arraySymmetricDifference(arr_lc_str_1, arr_lc_str_2) FROM tab ORDER BY id;

DROP TABLE tab;
