SELECT transform(NULL, ['', ''], ['', ''], *)
FROM
(
    SELECT NULL
); -- { serverError BAD_ARGUMENTS }
