-- Testing integer formats
select printf('%%d: %d', 123) = '%d: 123';
select printf('%%i: %i', 123) = '%i: 123';
select printf('%%u: %u', 123) = '%u: 123';
select printf('%%o: %o', 123) = '%o: 173';
select printf('%%x: %x', 123) = '%x: 7b';
select printf('%%X: %X', 123) = '%X: 7B';

-- Testing floating point formats
select printf('%%f: %f', 123.456) = '%f: 123.456000';
select printf('%%F: %F', 123.456) = '%F: 123.456000';
select printf('%%e: %e', 123.456) = '%e: 1.234560e+02';
select printf('%%E: %E', 123.456) = '%E: 1.234560E+02';
select printf('%%g: %g', 123.456) = '%g: 123.456';
select printf('%%G: %G', 123.456) = '%G: 123.456';
select printf('%%a: %a', 123.456) = '%a: 0x1.edd2f1a9fbe77p+6';
select printf('%%A: %A', 123.456) = '%A: 0X1.EDD2F1A9FBE77P+6';

-- Testing character formats
select printf('%%s: %s', 'abc') = '%s: abc';


-- Testing the %% specifier
select printf('%%%%: %%') = '%%: %';