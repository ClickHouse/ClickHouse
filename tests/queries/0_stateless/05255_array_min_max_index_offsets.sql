-- { echoOn }

SELECT arrayMinIndex(a), arrayMaxIndex(a)
FROM values(
    'a Array(Int32)',
    ([5, 3, 3, 7]),
    ([]),
    ([42]),
    ([2, 8, 1])
);
