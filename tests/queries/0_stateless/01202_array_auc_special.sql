SELECT arrayAUC([], []); -- { serverError 43 }
SELECT arrayAUC([1], [1]);
SELECT arrayAUC([1], []); -- { serverError 43 }
SELECT arrayAUC([], [1]); -- { serverError 43 }
SELECT arrayAUC([1, 2], [3]); -- { serverError 36 }
SELECT arrayAUC([1], [2, 3]); -- { serverError 36 }
SELECT arrayAUC([1, 1], [1, 1]);
SELECT arrayAUC([1, 1], [0, 0]);
SELECT arrayAUC([1, 1], [0, 1]);
SELECT arrayAUC([0, 1], [0, 1]);
SELECT arrayAUC([1, 0], [0, 1]);
SELECT arrayAUC([0, 0, 1], [0, 1, 1]);
SELECT arrayAUC([0, 1, 1], [0, 1, 1]);
SELECT arrayAUC([0, 1, 1], [0, 0, 1]);
