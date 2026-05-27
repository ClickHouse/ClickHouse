SELECT getMaxTableNameLengthForDatabase('default');
SELECT getMaxTableNameLengthForDatabase('default21');
SELECT getMaxTableNameLengthForDatabase(''); -- { serverError INCORRECT_DATA }
SET allow_experimental_drop_detached_table=1; SELECT getMaxTableNameLengthForDatabase('default');
SET allow_experimental_drop_detached_table=1; SELECT getMaxTableNameLengthForDatabase('default21');
