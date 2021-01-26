SELECT number, dictGet('executable_complex', 'a', (number, number)) AS a, dictGet('executable_complex', 'b', (number, number)) AS b FROM numbers(1000000) WHERE number = 999999;
SELECT number, dictGet('executable_complex_direct', 'a', (number, number)) AS a, dictGet('executable_complex_direct', 'b', (number, number)) AS b FROM numbers(1000000) WHERE number = 999999;
SELECT number, dictGet('executable_simple', 'a', number) AS a, dictGet('executable_simple', 'b', number) AS b FROM numbers(1000000) WHERE number = 999999;

SELECT 'Check implicit_key option';

SELECT dictGet('simple_executable_cache_dictionary_no_implicit_key', 'value', toUInt64(1));
SELECT dictGet('simple_executable_cache_dictionary_implicit_key', 'value', toUInt64(1));

SELECT dictGet('complex_executable_cache_dictionary_no_implicit_key', 'value', (toUInt64(1), 'FirstKey'));
SELECT dictGet('complex_executable_cache_dictionary_implicit_key', 'value', (toUInt64(1), 'FirstKey'));
