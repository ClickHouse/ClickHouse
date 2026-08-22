SELECT res
FROM
(
    SELECT
        arrayPartialReverseSort(2, if(number % 2, emptyArrayUInt64(), range(number))) AS arr,
        arrayResize(arr, if(empty(arr), 0, 2)) AS res
    FROM system.numbers
    LIMIT 7
);

SELECT res
FROM
(
    SELECT
        arrayPartialReverseSort(materialize(2), if(number % 2, emptyArrayUInt64(), range(number))) AS arr,
        arrayResize(arr, if(empty(arr), 0, 2)) AS res
    FROM system.numbers
    LIMIT 7
);
