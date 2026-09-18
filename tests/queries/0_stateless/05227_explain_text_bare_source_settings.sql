-- Bare form without actions: a SETTINGS clause directly after the statement stays with the
-- statement, exactly as in the parenthesized form and as for a SELECT source.
SELECT formatQuerySingleLine('EXPLAIN TEXT SHOW TABLES SETTINGS use_query_cache = true FORMAT JSONEachRow');
SELECT formatQuerySingleLine('EXPLAIN TEXT SHOW TABLES SETTINGS use_query_cache = true FORMAT JSONEachRow')
     = formatQuerySingleLine('EXPLAIN TEXT (SHOW TABLES SETTINGS use_query_cache = true) FORMAT JSONEachRow');
SELECT formatQuerySingleLine('EXPLAIN TEXT EXISTS TABLE t SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('EXPLAIN TEXT SELECT 1 SETTINGS max_threads = 1 SETTINGS max_block_size = 1');

-- SETTINGS after a trailing FORMAT follows the FORMAT and belongs to EXPLAIN TEXT.
SELECT formatQuerySingleLine('EXPLAIN TEXT SHOW TABLES FORMAT JSONEachRow SETTINGS max_threads = 1');

-- Source settings are preserved text: neither validated nor applied.
EXPLAIN TEXT SHOW TABLES SETTINGS explain_text_unknown_setting = 1 FORMAT JSONEachRow;
