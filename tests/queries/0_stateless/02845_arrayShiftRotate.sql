select '== arrayRotateLeft';
select arrayRotateLeft([1,2,3,4,5], 2);
select arrayRotateLeft([1,2,3,4,5], -2);
select arrayRotateLeft([1,2,3,4,5], 8);
select arrayRotateLeft(['H', 'e', 'l', 'l', 'o'], 2);
select arrayRotateLeft([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1);
select '';

select '== arrayRotateRight';
select arrayRotateRight([1,2,3,4,5], 2);
select arrayRotateRight([1,2,3,4,5], -2);
select arrayRotateRight([1,2,3,4,5], 8);
select arrayRotateRight(['H', 'e', 'l', 'l', 'o'], 2);
select arrayRotateRight([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1);
select '';

select '== arrayShiftLeft';
select arrayShiftLeft([1, 2, 3, 4, 5], 3);
select arrayShiftLeft([1, 2, 3, 4, 5], -3);
select arrayShiftLeft([1, 2, 3, 4, 5], 8);
select arrayShiftLeft(['a', 'b', 'c', 'd', 'e'], 3);
select arrayShiftLeft([[1, 2], [3, 4], [5, 6]], 2);
select arrayShiftLeft([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1);
select arrayShiftLeft([1, 2, 3, 4, 5], 3, 7);
select arrayShiftLeft(['a', 'b', 'c', 'd', 'e'], 3, 'foo');
select arrayShiftLeft([[1, 2], [3, 4], [5, 6]], 2, [7, 8]);
select arrayShiftLeft(CAST('[1, 2, 3, 4, 5, 6]', 'Array(UInt16)'), 1, 1000);
select '';

select '== arrayShiftRight';
select arrayShiftRight([1, 2, 3, 4, 5], 3);
select arrayShiftRight([1, 2, 3, 4, 5], -3);
select arrayShiftRight([1, 2, 3, 4, 5], 8);
select arrayShiftRight(['a', 'b', 'c', 'd', 'e'], 3);
select arrayShiftRight([[1, 2], [3, 4], [5, 6]], 2);
select arrayShiftRight([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], 1);
select arrayShiftRight([1, 2, 3, 4, 5], 3, 7);
select arrayShiftRight(['a', 'b', 'c', 'd', 'e'], 3, 'foo');
select arrayShiftRight([[1, 2], [3, 4], [5, 6]], 2, [7, 8]);
select arrayShiftRight(CAST('[1, 2, 3, 4, 5, 6]', 'Array(UInt16)'), 1, 1000);
select '';

select '== table';
drop table if exists t02845;
create table t02845 (a Array(UInt8), s Int16, d UInt8) engine = MergeTree order by d;
insert into t02845 values ([1,2,3,4,5,6], 2, 1),([1,2,3,4,5,6], 3, 2),([1,2,3,4], 3, 3),([4,8,15,16,23,42], 5, 4),([2, 7, 18, 28, 18, 28, 45, 90, 45], 7, 5),([3, 14, 159, 26, 5], 11, 6);

select '== table with constants';
select '-- arrayRotateLeft';
select arrayRotateLeft(a, 2) from t02845;
select '-- arrayRotateRight';
select arrayRotateRight(a, 2) from t02845;
select '-- arrayShiftLeft';
select arrayShiftLeft(a, 3) from t02845;
select '-- arrayShiftRight';
select arrayShiftRight(a, 3) from t02845;

select '== table with constants and defaults';
select '-- arrayShiftLeft';
select arrayShiftLeft(a, 3, 7) from t02845;
select '-- arrayShiftRight';
select arrayShiftRight(a, 3, 7) from t02845;

select '== table values';
select '-- arrayRotateLeft';
select arrayRotateLeft(a, s) from t02845;
select '-- arrayRotateRight';
select arrayRotateRight(a, s) from t02845;
select '-- arrayShiftLeft';
select arrayShiftLeft(a, s, d) from t02845;
select '-- arrayShiftRight';
select arrayShiftRight(a, s, d) from t02845;

select '== problematic cast cases';
select arrayShiftLeft([30000], 3, 5);
select arrayShiftLeft([[1]], 3, []);
select arrayShiftLeft(['foo'], 3, 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select arrayShiftLeft([1], 3, 'foo'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
