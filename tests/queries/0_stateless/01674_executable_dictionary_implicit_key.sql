SELECT dictGet('simple_executable_cache_dictionary_no_implicit_key', 'value', toUInt64(1));
SELECT dictGet('simple_executable_cache_dictionary_implicit_key', 'value', toUInt64(1));

SELECT dictGet('complex_executable_cache_dictionary_no_implicit_key', 'value', (toUInt64(1), 'FirstKey'));
SELECT dictGet('complex_executable_cache_dictionary_implicit_key', 'value', (toUInt64(1), 'FirstKey'));
