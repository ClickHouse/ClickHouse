SELECT dumpColumnStructure([]);
SELECT dumpColumnStructure([[[]]]);
SELECT DISTINCT dumpColumnStructure([[], [1]]) FROM numbers(2);
