SELECT arrayFill(x -> (x < 10), []);
SELECT arrayFill(x -> (x < 10), emptyArrayUInt8());
SELECT arrayFill(x -> 1, []);
SELECT arrayFill(x -> 0, []);
