SELECT getMaxTableNameLengthForDatabase('default');
SELECT getMaxTableNameLengthForDatabase('default21');
SELECT getMaxTableNameLengthForDatabase(''); -- { serverError INCORRECT_DATA }
