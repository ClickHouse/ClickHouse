-- Tags: no-replicated-database, no-parallel-replicas, no-random-settings
-- Output may differ

SET output_format_write_statistics = 0;
SELECT number FROM numbers(100) ORDER BY number LIMIT -5 FORMAT JSON;
SELECT number FROM numbers(100) ORDER BY number LIMIT 0.1 FORMAT JSON;
