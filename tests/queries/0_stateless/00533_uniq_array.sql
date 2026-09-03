SELECT uniqArray([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniqArray([0, 1, 1], [0, 1, 1], [0, 1, 0]);
SELECT uniqExactArray([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniqExactArray([0, 1, 1], [0, 1, 1], [0, 1, 0]);
SELECT uniqUpToArray(10)([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniqUpToArray(10)([0, 1, 1], [0, 1, 1], [0, 1, 0]);
