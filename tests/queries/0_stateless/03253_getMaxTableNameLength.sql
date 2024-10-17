SELECT getMaxTableNameLengthForDatabase('default');
SELECT getMaxTableNameLengthForDatabase('default21');
SELECT getMaxTableNameLengthForDatabase(''); -- { INCORRECT_DATA }
