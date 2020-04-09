SELECT substring('hello', []); -- { serverError 43 }
SELECT substring('hello', 1, []); -- { serverError 43 }
SELECT substring(materialize('hello'), -1, -1); -- { serverError 69 }
SELECT substring(materialize('hello'), 0); -- { serverError 135 }