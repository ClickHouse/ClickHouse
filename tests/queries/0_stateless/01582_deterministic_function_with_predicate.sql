EXPLAIN SYNTAX SELECT count(*) FROM ( SELECT number FROM ( SELECT number FROM numbers(1000000) ) WHERE rand64() < (0.01 * 18446744073709552000.)) SETTINGS allow_experimental_analyzer = 0;
EXPLAIN SYNTAX SELECT count(*) FROM ( SELECT number FROM ( SELECT number FROM numbers(1000000) ) WHERE rand64() < (0.01 * 18446744073709552000.)) SETTINGS allow_experimental_analyzer = 1;
