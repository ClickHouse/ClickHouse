-- { echoOn }

SELECT ltrim('   leading   '), trimLeft('   leading   ');
SELECT ltrim('xxleadingxx', 'x'), trimLeft('xxleadingxx', 'x');

SELECT rtrim('   trailing   '), trimRight('   trailing   ');
SELECT rtrim('xxtrailingxx', 'x'), trimRight('xxtrailingxx', 'x');

SELECT trim('   both   '), trimBoth('   both   ');
SELECT trim('$$both$$', '$'), trimBoth('$$both$$', '$');
SELECT TRIM('   both   '), trimBoth('   both   ');

SELECT TRIM(BOTH '$' FROM '$$both$$'), trimBoth('$$both$$', '$');
SELECT TRIM(LEADING '$' FROM '$$both$$'), trimLeft('$$both$$', '$');
SELECT TRIM(TRAILING '$' FROM '$$both$$'), trimRight('$$both$$', '$');
SELECT TRIM(BOTH '' FROM 'xx'), trimBoth('xx', '');
SELECT TRIM(LEADING '' FROM 'xx'), trimLeft('xx', '');
SELECT TRIM(TRAILING '' FROM 'xx'), trimRight('xx', '');
SELECT TRIM(BOTH concat('$', '$') FROM '$$both$$'), trimBoth('$$both$$', '$$');

SELECT ltrim('\t  abc', '\t '), trimLeft('\t  abc', '\t ');
SELECT rtrim('abc\t  ', '\t '), trimRight('abc\t  ', '\t ');

SELECT TrIm('  x  '), trimBoth('  x  ');

SELECT LTRIM('  x  '), trimLeft('  x  ');
SELECT LtRiM('  x  '), trimLeft('  x  ');

SELECT RTRIM('  x  '), trimRight('  x  ');
SELECT RtRiM('  x  '), trimRight('  x  ');
