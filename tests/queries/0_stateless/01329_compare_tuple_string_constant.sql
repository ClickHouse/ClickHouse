SELECT tuple(1) < ''; -- { serverError 27 }
SELECT tuple(1) < materialize(''); -- { serverError 386 }
SELECT (1, 2) < '(1,3)';
SELECT (1, 2) < '(1, 1)';
