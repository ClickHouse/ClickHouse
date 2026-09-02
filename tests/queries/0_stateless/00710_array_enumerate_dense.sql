-- empty result set
SELECT a FROM (SELECT groupArray(intDiv(number, 54321)) AS a, arrayUniq(a) AS u, arrayEnumerateDense(a) AS arr FROM (SELECT number FROM system.numbers LIMIT 1000000) GROUP BY intHash32(number) % 100000) where u <> arrayReverseSort(arr)[1];

SELECT arrayEnumerateDense([[1], [2], [34], [1]]);
SELECT arrayEnumerateDense([(1, 2), (3, 4), (1, 2)]);
