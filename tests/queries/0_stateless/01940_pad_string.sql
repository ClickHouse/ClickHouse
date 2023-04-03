SELECT 'leftPad';
SELECT leftPad('abc', 0);
SELECT leftPad('abc', 1);
SELECT leftPad('abc', 2);
SELECT leftPad('abc', 3);
SELECT leftPad('abc', 4);
SELECT leftPad('abc', 5);
SELECT leftPad('abc', 10);

SELECT leftPad('abc', 2, '*');
SELECT leftPad('abc', 4, '*');
SELECT leftPad('abc', 5, '*');
SELECT leftPad('abc', 10, '*');
SELECT leftPad('abc', 2, '*.');
SELECT leftPad('abc', 4, '*.');
SELECT leftPad('abc', 5, '*.');
SELECT leftPad('abc', 10, '*.');

SELECT 'leftPadUTF8';
SELECT leftPad('абвг', 2);
SELECT leftPadUTF8('абвг', 2);
SELECT leftPad('абвг', 4);
SELECT leftPadUTF8('абвг', 4);
SELECT leftPad('абвг', 12, 'ЧАС');
SELECT leftPadUTF8('абвг', 12, 'ЧАС');

SELECT 'rightPad';
SELECT rightPad('abc', 0);
SELECT rightPad('abc', 1);
SELECT rightPad('abc', 2);
SELECT rightPad('abc', 3);
SELECT rightPad('abc', 4);
SELECT rightPad('abc', 5);
SELECT rightPad('abc', 10);

SELECT rightPad('abc', 2, '*');
SELECT rightPad('abc', 4, '*');
SELECT rightPad('abc', 5, '*');
SELECT rightPad('abc', 10, '*');
SELECT rightPad('abc', 2, '*.');
SELECT rightPad('abc', 4, '*.');
SELECT rightPad('abc', 5, '*.');
SELECT rightPad('abc', 10, '*.');

SELECT 'rightPadUTF8';
SELECT rightPad('абвг', 2);
SELECT rightPadUTF8('абвг', 2);
SELECT rightPad('абвг', 4);
SELECT rightPadUTF8('абвг', 4);
SELECT rightPad('абвг', 12, 'ЧАС');
SELECT rightPadUTF8('абвг', 12, 'ЧАС');

SELECT 'numbers';
SELECT rightPad(leftPad(toString(number), number, '_'), number*2, '^') FROM numbers(7);
