-- Regression test: the hidden inner query of `obfuscate` is analyzed from scratch on the node
-- that executes it, so positional arguments (`GROUP BY 1` / `ORDER BY 1`) must be resolved there
-- even when the outer context is a secondary (remote-shard) query, where positional-argument
-- resolution is normally skipped as already done by the initiator. With the bug, the remote
-- execution left `GROUP BY 1` as a literal and failed with `NOT_AN_AGGREGATE`.
SET enable_analyzer = 1;

SELECT 'local';
SELECT * FROM obfuscate(SELECT number % 2 AS k, count() AS c FROM numbers(4) GROUP BY 1 ORDER BY 1) LIMIT 4 SETTINGS obfuscate_seed = 'positional';
SELECT 'remote';
SELECT * FROM remote('127.0.0.2', obfuscate(SELECT number % 2 AS k, count() AS c FROM numbers(4) GROUP BY 1 ORDER BY 1)) LIMIT 4 SETTINGS obfuscate_seed = 'positional';
