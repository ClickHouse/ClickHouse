SELECT lambda(tuple(1), 1); -- {serverError BAD_ARGUMENTS, TYPE_MISMATCH} argument is not identifier
SELECT lambda(tuple(1, 2), materialize(1) + x); -- {serverError BAD_ARGUMENTS, TYPE_MISMATCH} argument is not identifier
