SELECT arrayAUC([], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAUC([1], [1]);
SELECT arrayAUC([1], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAUC([], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAUC([1, 2], [3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayAUC([1], [2, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayAUC([1, 1], [1, 1]);
SELECT arrayAUC([1, 1], [0, 0]);
SELECT arrayAUC([1, 1], [0, 1]);
SELECT arrayAUC([0, 1], [0, 1]);
SELECT arrayAUC([1, 0], [0, 1]);
SELECT arrayAUC([0, 0, 1], [0, 1, 1]);
SELECT arrayAUC([0, 1, 1], [0, 1, 1]);
SELECT arrayAUC([0, 1, 1], [0, 0, 1]);
SELECT arrayAUC([], [], false); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAUC([1], [1], false);
SELECT arrayAUC([1], [], false); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAUC([], [1], false); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAUC([1, 2], [3], false); -- { serverError BAD_ARGUMENTS }
SELECT arrayAUC([1], [2, 3], false); -- { serverError BAD_ARGUMENTS }
SELECT arrayAUC([1, 1], [1, 1], false);
SELECT arrayAUC([1, 1], [0, 0], false);
SELECT arrayAUC([1, 1], [0, 1], false);
SELECT arrayAUC([0, 1], [0, 1], false);
SELECT arrayAUC([1, 0], [0, 1], false);
SELECT arrayAUC([0, 0, 1], [0, 1, 1], false);
SELECT arrayAUC([0, 1, 1], [0, 1, 1], false);
SELECT arrayAUC([0, 1, 1], [0, 0, 1], false);
