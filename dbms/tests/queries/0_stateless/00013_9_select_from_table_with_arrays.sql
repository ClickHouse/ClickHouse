SELECT s, arr, a, mapped FROM arrays_test ARRAY JOIN arr AS a, arrayMap(x -> x + 1, arr) AS mapped
