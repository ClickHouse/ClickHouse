SELECT tryBase64Decode(( SELECT countSubstrings(toModifiedJulianDayOrNull('\0'), '') ) AS n, ( SELECT regionIn('l. ') ) AS srocpnuv); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT countSubstrings(toModifiedJulianDayOrNull('\0'), ''); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT countSubstrings(toInt32OrNull('123qwe123'), ''); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'Ok.';
