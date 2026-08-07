SELECT (1,'') IN ((-1,''));
SELECT (1,'') IN ((1,''));
SELECT (1,'') IN (-1,'');
SELECT (1,'') IN (1,'');
SELECT (1,'') IN ((-1,''),(1,''));

SELECT (number, toString(number)) IN ((1, '1'), (-1, '-1')) FROM system.numbers LIMIT 10;
SELECT (number - 1, toString(number - 1)) IN ((1, '1'), (-1, '-1')) FROM system.numbers LIMIT 10;
