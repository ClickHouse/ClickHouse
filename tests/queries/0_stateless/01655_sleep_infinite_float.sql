SELECT sleep(nan); -- { serverError BAD_ARGUMENTS }
SELECT sleep(inf); -- { serverError BAD_ARGUMENTS }
