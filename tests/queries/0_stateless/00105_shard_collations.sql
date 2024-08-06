-- Tags: shard, no-fasttest

SELECT 'Русский (default)';
SELECT arrayJoin(['а', 'я', 'ё', 'А', 'Я', 'Ё']) AS x ORDER BY x;

SELECT 'Русский (ru)';
SELECT arrayJoin(['а', 'я', 'ё', 'А', 'Я', 'Ё']) AS x ORDER BY x COLLATE 'ru';

SELECT 'Русский (ru distributed)';
SELECT arrayJoin(['а', 'я', 'ё', 'А', 'Я', 'Ё']) AS x FROM remote('127.0.0.{2,3}', system, one) ORDER BY x COLLATE 'ru';

SELECT 'Türk (default)';
SELECT arrayJoin(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'ç', 'd', 'e', 'f', 'g', 'ğ', 'h', 'ı', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'ö', 'p', 'r', 's', 'ş', 't', 'u', 'ü', 'v', 'y', 'z', 'A', 'B', 'C', 'Ç', 'D', 'E', 'F', 'G', 'Ğ', 'H', 'I', 'İ', 'J', 'K', 'L', 'M', 'N', 'O', 'Ö', 'P', 'R', 'S', 'Ş', 'T', 'U', 'Ü', 'V', 'Y', 'Z']) AS x ORDER BY x;

SELECT 'Türk (tr)';
SELECT arrayJoin(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'ç', 'd', 'e', 'f', 'g', 'ğ', 'h', 'ı', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'ö', 'p', 'r', 's', 'ş', 't', 'u', 'ü', 'v', 'y', 'z', 'A', 'B', 'C', 'Ç', 'D', 'E', 'F', 'G', 'Ğ', 'H', 'I', 'İ', 'J', 'K', 'L', 'M', 'N', 'O', 'Ö', 'P', 'R', 'S', 'Ş', 'T', 'U', 'Ü', 'V', 'Y', 'Z']) AS x ORDER BY x COLLATE 'tr';

SELECT 'english (default)';
SELECT arrayJoin(['A', 'c', 'Z', 'Q', 'e']) AS x ORDER BY x;
SELECT 'english (en_US)';
SELECT arrayJoin(['A', 'c', 'Z', 'Q', 'e']) AS x ORDER BY x COLLATE 'en_US';
SELECT 'english (en)';
SELECT arrayJoin(['A', 'c', 'Z', 'Q', 'e']) AS x ORDER BY x COLLATE 'en';

SELECT 'español (default)';
SELECT arrayJoin(['F', 'z', 'J', 'Ñ']) as x ORDER BY x;
SELECT 'español (es)';
SELECT arrayJoin(['F', 'z', 'J', 'Ñ']) as x ORDER BY x COLLATE 'es';

SELECT 'Український (default)';
SELECT arrayJoin(['ґ', 'ї', 'І', 'Б']) as x ORDER BY x;
SELECT 'Український (uk)';
SELECT arrayJoin(['ґ', 'ї', 'І', 'Б']) as x ORDER BY x COLLATE 'uk';

SELECT 'Русский (ru group by)';
SELECT x, n FROM (SELECT ['а', 'я', 'ё', 'А', 'Я', 'Ё'] AS arr) ARRAY JOIN arr AS x, arrayEnumerate(arr) AS n ORDER BY x COLLATE 'ru', n;

--- Const expression
SELECT 'ζ' as x ORDER BY x COLLATE 'el';

-- check order by const with collation
SELECT number FROM numbers(2) ORDER BY 'x' COLLATE 'el';

-- check const and non const columns in order
SELECT number FROM numbers(11) ORDER BY 'x', toString(number), 'y' COLLATE 'el';

--- Trash locales
SELECT '' as x ORDER BY x COLLATE 'qq'; --{serverError UNSUPPORTED_COLLATION_LOCALE}
SELECT '' as x ORDER BY x COLLATE 'qwe'; --{serverError UNSUPPORTED_COLLATION_LOCALE}
SELECT '' as x ORDER BY x COLLATE 'some_non_existing_locale'; --{serverError UNSUPPORTED_COLLATION_LOCALE}
SELECT '' as x ORDER BY x COLLATE 'ру'; --{serverError UNSUPPORTED_COLLATION_LOCALE}
