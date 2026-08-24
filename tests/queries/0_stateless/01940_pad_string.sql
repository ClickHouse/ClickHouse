SELECT 'leftPad';
SELECT leftPad('abc', 0), leftPad('abc', 0::Int32);
SELECT leftPad('abc', 1), leftPad('abc', 1::Int32);
SELECT leftPad('abc', 2), leftPad('abc', 2::Int32);
SELECT leftPad('abc', 3), leftPad('abc', 3::Int32);
SELECT leftPad('abc', 4), leftPad('abc', 4::Int32);
SELECT leftPad('abc', 5), leftPad('abc', 5::Int32);
SELECT leftPad('abc', 10), leftPad('abc', 10::Int32);

SELECT leftPad('abc', 2, '*'),  leftPad('abc', 2::Int32, '*');
SELECT leftPad('abc', 4, '*'),  leftPad('abc', 4::Int32, '*');
SELECT leftPad('abc', 5, '*'),  leftPad('abc', 5::Int32, '*');
SELECT leftPad('abc', 10, '*'), leftPad('abc', 10::Int32, '*');
SELECT leftPad('abc', 2, '*.'), leftPad('abc', 2::Int32, '*.');
SELECT leftPad('abc', 4, '*.'), leftPad('abc', 4::Int32, '*.');
SELECT leftPad('abc', 5, '*.'), leftPad('abc', 5::Int32, '*.');
SELECT leftPad('abc', 10, '*.'),leftPad('abc', 10::Int32, '*.');

SELECT 'leftPadUTF8';
SELECT leftPad('абвг', 2), leftPad('абвг', 2::Int32);
SELECT leftPadUTF8('абвг', 2), leftPadUTF8('абвг', 2::Int32);
SELECT leftPad('абвг', 4), leftPad('абвг', 4::Int32);
SELECT leftPadUTF8('абвг', 4), leftPadUTF8('абвг', 4::Int32);
SELECT leftPad('абвг', 12, 'ЧАС'), leftPad('абвг', 12::Int32, 'ЧАС');
SELECT leftPadUTF8('абвг', 12, 'ЧАС'), leftPadUTF8('абвг', 12::Int32, 'ЧАС');

SELECT 'rightPad';
SELECT rightPad('abc', 0), rightPad('abc', 0::Int32);
SELECT rightPad('abc', 1), rightPad('abc', 1::Int32);
SELECT rightPad('abc', 2), rightPad('abc', 2::Int32);
SELECT rightPad('abc', 3), rightPad('abc', 3::Int32);
SELECT rightPad('abc', 4), rightPad('abc', 4::Int32);
SELECT rightPad('abc', 5), rightPad('abc', 5::Int32);
SELECT rightPad('abc', 10), rightPad('abc', 10::Int32);

SELECT rightPad('abc', 2, '*'), rightPad('abc', 2::Int32, '*');
SELECT rightPad('abc', 4, '*'), rightPad('abc', 4::Int32, '*');
SELECT rightPad('abc', 5, '*'), rightPad('abc', 5::Int32, '*');
SELECT rightPad('abc', 10, '*'), rightPad('abc', 10::Int32, '*');
SELECT rightPad('abc', 2, '*.'), rightPad('abc', 2::Int32, '*.');
SELECT rightPad('abc', 4, '*.'), rightPad('abc', 4::Int32, '*.');
SELECT rightPad('abc', 5, '*.'), rightPad('abc', 5::Int32, '*.');
SELECT rightPad('abc', 10, '*.'), rightPad('abc', 10::Int32, '*.');

SELECT 'rightPadUTF8';
SELECT rightPad('абвг', 2), rightPad('абвг', 2::Int32);
SELECT rightPadUTF8('абвг', 2), rightPadUTF8('абвг', 2::Int32);
SELECT rightPad('абвг', 4), rightPad('абвг', 4::Int32);
SELECT rightPadUTF8('абвг', 4), rightPadUTF8('абвг', 4::Int32);
SELECT rightPad('абвг', 12, 'ЧАС'), rightPad('абвг', 12::Int32, 'ЧАС');
SELECT rightPadUTF8('абвг', 12, 'ЧАС'), rightPadUTF8('абвг', 12::Int32, 'ЧАС');

SELECT 'numbers';
SELECT rightPad(leftPad(toString(number), number, '_'), number*2, '^') FROM numbers(7);
SELECT rightPad(leftPad(toString(number), number::Int64, '_'), number::Int64*2, '^') FROM numbers(7);
