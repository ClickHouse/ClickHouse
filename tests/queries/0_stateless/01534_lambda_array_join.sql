SELECT arrayMap(x -> concat(x, concat(arrayJoin([1]), x, NULL), ''), [1]);
SELECT arrayMap(x -> arrayJoin([1]), [1, 2]);

SELECT
        arrayJoin(arrayMap(x -> reinterpretAsUInt8(substring(randomString(range(randomString(1048577), NULL), arrayJoin(arrayMap(x -> reinterpretAsUInt8(substring(randomString(range(NULL), 65537), 255)), range(1))), substring(randomString(NULL), x + 7), '257'), 1025)), range(7))) AS byte,
        count() AS c
    FROM numbers(10)
    GROUP BY
        arrayMap(x -> reinterpretAsUInt8(substring(randomString(randomString(range(randomString(255), NULL)), NULL), NULL)), range(3)),
        randomString(range(randomString(1048577), NULL), NULL),
        byte
    ORDER BY byte ASC;
