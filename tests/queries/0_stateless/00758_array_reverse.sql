SELECT reverse([NULL, '\0']);
SELECT reverse([NULL, 123, NULL]);
SELECT reverse([toFixedString('Hello', 5), NULL]);
SELECT reverse(['Hello', 'world']);
SELECT reverse(['Hello', NULL, 'world']);
SELECT reverse([NULL, NULL, NULL]);
SELECT reverse([[], [''], [' ']]);
SELECT reverse([[], [''], [NULL]]);
SELECT reverse([(1, 'Hello', []), (nan, 'World', [NULL])]);
SELECT reverse(NULL);
SELECT reverse([]);
SELECT reverse([[[[]]]]);
SELECT arrayReverse(materialize([toDecimal32('1.23', 2), toDecimal32('-4.56', 2)]));
SELECT arrayReverse(materialize([toDecimal64('1.234', 3), NULL, toDecimal64('-5.678', 3)]));
SELECT arrayReverse(materialize([toDecimal128('1.2345', 4), toDecimal128('-6.7890', 4)]));
SELECT arrayReverse(materialize([toDecimal256('1.23456', 5), toDecimal256('-7.89012', 5)]));

SET send_logs_level = 'fatal';
SELECT '[RE7', ( SELECT '\0' ) AS riwwq, ( SELECT reverse([( SELECT bitTestAll(NULL) ) , ( SELECT '\0' ) AS ddfweeuy]) ) AS xuvv, '', ( SELECT * FROM file() ) AS wqgdswyc, ( SELECT * FROM file() ); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
