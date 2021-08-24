SELECT sleep(nan); -- { serverError 36 }
SELECT sleep(inf); -- { serverError 36 }
