SELECT arrayROCAUC([], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([1], [1]);
SELECT arrayROCAUC([1], []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([1, 2], [3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayROCAUC([1], [2, 3]); -- { serverError BAD_ARGUMENTS }
SELECT arrayROCAUC([1, 1], [1, 1]);
SELECT arrayROCAUC([1, 1], [0, 0]);
SELECT arrayROCAUC([1, 1], [0, 1]);
SELECT arrayROCAUC([0, 1], [0, 1]);
SELECT arrayROCAUC([1, 0], [0, 1]);
SELECT arrayROCAUC([0, 0, 1], [0, 1, 1]);
SELECT arrayROCAUC([0, 1, 1], [0, 1, 1]);
SELECT arrayROCAUC([0, 1, 1], [0, 0, 1]);
SELECT arrayROCAUC([], [], true); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([1], [1], true);
SELECT arrayROCAUC([1], [], true); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([], [1], true); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([1, 2], [3], true); -- { serverError BAD_ARGUMENTS }
SELECT arrayROCAUC([1], [2, 3], true); -- { serverError BAD_ARGUMENTS }
SELECT arrayROCAUC([1, 1], [1, 1], true);
SELECT arrayROCAUC([1, 1], [0, 0], true);
SELECT arrayROCAUC([1, 1], [0, 1], true);
SELECT arrayROCAUC([0, 1], [0, 1], true);
SELECT arrayROCAUC([1, 0], [0, 1], true);
SELECT arrayROCAUC([0, 0, 1], [0, 1, 1], true);
SELECT arrayROCAUC([0, 1, 1], [0, 1, 1], true);
SELECT arrayROCAUC([0, 1, 1], [0, 0, 1], true);
SELECT arrayROCAUC([], [], false); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([1], [1], false);
SELECT arrayROCAUC([1], [], false); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([], [1], false); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([1, 2], [3], false); -- { serverError BAD_ARGUMENTS }
SELECT arrayROCAUC([1], [2, 3], false); -- { serverError BAD_ARGUMENTS }
SELECT arrayROCAUC([1, 1], [1, 1], false);
SELECT arrayROCAUC([1, 1], [0, 0], false);
SELECT arrayROCAUC([1, 1], [0, 1], false);
SELECT arrayROCAUC([0, 1], [0, 1], false);
SELECT arrayROCAUC([1, 0], [0, 1], false);
SELECT arrayROCAUC([0, 0, 1], [0, 1, 1], false);
SELECT arrayROCAUC([0, 1, 1], [0, 1, 1], false);
SELECT arrayROCAUC([0, 1, 1], [0, 0, 1], false);
SELECT arrayROCAUC([0, 1, 1], [0, 0, 1], false, [0, 0, 0], true); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayROCAUC([0, 1, 1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayROCAUC([0, 1, 1], [0, 0, 1], 'false'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayROCAUC([0, 1, 1], [0, 0, 1], 4); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
