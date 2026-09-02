DROP TABLE IF EXISTS arrays_test;

CREATE TABLE arrays_test
(
    s String,
    arr1 Array(UInt8),
    map1 Map(UInt8, String),
    map2 Map(UInt8, String)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2], map(1, '1', 2, '2'), map(1, '1')), ('World', [3,4,5], map(3, '3', 4, '4', 5, '5'), map(3, '3', 4, '4')), ('Goodbye', [], map(), map());


select s, arr1, map1 from arrays_test array join arr1, map1 settings enable_unaligned_array_join = 1;

select s, arr1, map1 from arrays_test left array join arr1, map1 settings enable_unaligned_array_join = 1;

select s, map1 from arrays_test array join map1;

select s, map1 from arrays_test left array join map1;

select s, map1, map2 from arrays_test array join map1, map2 settings enable_unaligned_array_join = 1; 

select s, map1, map2 from arrays_test left array join map1, map2 settings enable_unaligned_array_join = 1; 
