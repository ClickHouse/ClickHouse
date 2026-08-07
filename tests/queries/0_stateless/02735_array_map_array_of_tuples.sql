SELECT arrayMap((x) -> x, [tuple(1)]);
SELECT arrayMap((x) -> x.1, [tuple(1)]);
SELECT arrayMap((x) -> x.1 + x.2, [tuple(1, 2)]);
SELECT arrayMap((x, y) -> x + y, [tuple(1, 2)]);
