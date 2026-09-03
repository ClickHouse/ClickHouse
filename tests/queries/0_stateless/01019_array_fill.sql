SELECT arrayFill(x -> 0, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT arrayReverseFill(x -> 0, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT arrayFill(x -> 1, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT arrayReverseFill(x -> 1, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);

SELECT arrayFill(x -> x < 10, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT arrayReverseFill(x -> x < 10, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16]);
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]);
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]);
SELECT arrayFill((x, y) -> y, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16], [0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0]);
SELECT arrayReverseFill((x, y) -> y, [1, 2, 3, 11, 12, 13, 4, 5, 6, 14, 15, 16], [0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0]);
