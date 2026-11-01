-- histogram() must produce identical results regardless of input order when count <= max_bins.
-- Bug: compress() had an early return before sorting when size <= max_bins,
-- so unique() operated on unsorted data and incorrectly merged non-adjacent bins.

SELECT 'ascending vs unsorted 5 values';
SELECT histogram(10)(x) FROM (SELECT number + 1 AS x FROM numbers(5));
SELECT histogram(10)(x) FROM (SELECT arrayJoin([5,3,1,4,2]) AS x);

SELECT 'ascending vs descending 5 values';
SELECT histogram(10)(x) FROM (SELECT number + 1 AS x FROM numbers(5));
SELECT histogram(10)(x) FROM (SELECT 5 - number AS x FROM numbers(5));

SELECT 'ascending vs shuffled 8 values';
SELECT histogram(10)(x) FROM (SELECT number + 1 AS x FROM numbers(8));
SELECT histogram(10)(x) FROM (SELECT arrayJoin([4,7,2,8,1,6,3,5]) AS x);
