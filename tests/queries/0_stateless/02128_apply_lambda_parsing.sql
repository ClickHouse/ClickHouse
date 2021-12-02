WITH * APPLY lambda(e); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY lambda(); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY lambda(1); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY lambda(x); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY lambda(range(1)); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY lambda(range(x)); -- { clientError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * APPLY lambda(1, 2); -- { clientError TYPE_MISMATCH }
SELECT * APPLY lambda(x, y); -- { clientError TYPE_MISMATCH }
SELECT * APPLY lambda((x, y), 2); -- { clientError BAD_ARGUMENTS }
SELECT * APPLY lambda((x, y), x + y); -- { clientError BAD_ARGUMENTS }
SELECT * APPLY lambda(tuple(1), 1); -- { clientError BAD_ARGUMENTS }
SELECT * APPLY lambda(tuple(x), 1) FROM numbers(5);
SELECT * APPLY lambda(tuple(x), x + 1) FROM numbers(5);
