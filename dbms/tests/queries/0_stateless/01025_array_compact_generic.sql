SELECT arrayCompact([]);
SELECT arrayCompact([1, 1, nan, nan, 2, 2, 2]);
SELECT arrayCompact([1, 1, nan, nan, -nan, 2, 2, 2]);
SELECT arrayCompact([1, 1, NULL, NULL, 2, 2, 2]);
SELECT arrayCompact(['hello', '', '', '', 'world', 'world']);
SELECT arrayCompact([[[]], [[], []], [[], []], [[]]]);
SELECT arrayCompact(x -> toString(intDiv(x, 3)), range(number)) FROM numbers(10);
