SET enable_analyzer = 1;

SELECT transform(NULL, ['', ''], ['', ''], *)
FROM
(
    SELECT NULL
); -- { serverError BAD_ARGUMENTS }
