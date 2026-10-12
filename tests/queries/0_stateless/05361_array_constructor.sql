-- Test array constructor syntax array[...]
SELECT array[1, 2, 3];
SELECT array[1.5, 2.5, 3.5];
SELECT array['a', 'b', 'c'];
SELECT array[];
SELECT array[1+1, 2*3, 4-1];
SELECT array[1, 2, 3] AS my_array;
SELECT array[array[1, 2], array[3, 4]];
SELECT array[NULL, 1, 2];
SELECT array[10, 20, 30] AS arr, arr[1] AS first_element;
