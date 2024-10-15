-- Testing integer formats
select printf('%%d: %d', 123);
select printf('%%d: %d', -123);
select printf('%%d: %d', 0);
select printf('%%d: %d', 9223372036854775807);
select printf('%%i: %i', 123);
select printf('%%u: %u', 123);
select printf('%%o: %o', 123);
select printf('%%x: %x', 123);
select printf('%%X: %X', 123);

-- Testing floating point formats
select printf('%%f: %f', 0.0);
select printf('%%f: %f', 123.456);
select printf('%%f: %f', -123.456);
select printf('%%F: %F', 123.456);
select printf('%%e: %e', 123.456);
select printf('%%E: %E', 123.456);
select printf('%%g: %g', 123.456);
select printf('%%G: %G', 123.456);
select printf('%%a: %a', 123.456);
select printf('%%A: %A', 123.456);

-- Testing character formats
select printf('%%s: %s', 'abc');
SELECT printf('%%s: %s', '\n\t') FORMAT PrettyCompact;
select printf('%%s: %s', '');

-- Testing the %% specifier
select printf('%%%%: %%');

-- Testing integer formats with precision
select printf('%%.5d: %.5d', 123);

-- Testing floating point formats with precision
select printf('%%.2f: %.2f', 123.456);
select printf('%%.2e: %.2e', 123.456);
select printf('%%.2g: %.2g', 123.456);

-- Testing character formats with precision
select printf('%%.2s: %.2s', 'abc');

select printf('%%X: %X', 123.123); -- { serverError BAD_ARGUMENTS }
select printf('%%A: %A', 'abc'); -- { serverError BAD_ARGUMENTS }
select printf('%%s: %s', 100); -- { serverError BAD_ARGUMENTS }
select printf('%%n: %n', 100); -- { serverError BAD_ARGUMENTS }
select printf('%%f: %f', 0); -- { serverError BAD_ARGUMENTS }
