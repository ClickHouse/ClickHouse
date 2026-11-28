-- Tags: distributed

SET prefer_localhost_replica = 0;

-- A table column referenced only inside `indexHint` under a window function: the shards and the
-- initiator must agree on the mergeable-stage header, and the remote rows must be counted.

DROP TABLE IF EXISTS d_hint_only;
DROP TABLE IF EXISTS t_hint_only;
DROP TABLE IF EXISTS t_hint_only_mem;

CREATE TABLE t_hint_only (k UInt32, v UInt32, a UInt32 ALIAS k, h UInt8 ALIAS indexHint(k = 7)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_hint_only SELECT intDiv(number, 10), number FROM numbers(100);
CREATE TABLE t_hint_only_mem (k UInt32, v UInt32, a UInt32 ALIAS k, h UInt8 ALIAS indexHint(k = 7)) ENGINE = Memory;
INSERT INTO t_hint_only_mem SELECT intDiv(number, 10), number FROM numbers(100);
CREATE TABLE d_hint_only AS t_hint_only ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_hint_only);

SELECT 'qualify, remote';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(k = 7));
SELECT 'qualify, Distributed';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM d_hint_only QUALIFY indexHint(k = 7));
SELECT 'row_number';
SELECT count(), max(c) FROM (SELECT row_number() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(k = 7));
-- A serialized plan does not prune granules by indexHint on the shards; these two compare against a local read.
SELECT 'where';
SELECT
    (SELECT count() FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) WHERE indexHint(k = 7)))
        = 2 * (SELECT count() FROM (SELECT count() OVER () AS c FROM t_hint_only WHERE indexHint(k = 7)))
    AND (SELECT count() FROM (SELECT count() OVER () AS c FROM t_hint_only WHERE indexHint(k = 7))) > 0 SETTINGS serialize_query_plan = 0;
SELECT 'prewhere';
SELECT
    (SELECT count() FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) PREWHERE indexHint(k = 7)))
        = 2 * (SELECT count() FROM (SELECT count() OVER () AS c FROM t_hint_only PREWHERE indexHint(k = 7)))
    AND (SELECT count() FROM (SELECT count() OVER () AS c FROM t_hint_only PREWHERE indexHint(k = 7))) > 0 SETTINGS serialize_query_plan = 0;
SELECT 'Memory';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only_mem) QUALIFY indexHint(k = 7));
SELECT 'view';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', view(SELECT 1 AS one, number FROM numbers(100))) QUALIFY indexHint(one = 1));
SELECT 'nested indexHint';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(k > 0 AND indexHint(k = 7)));
SELECT 'nested indexHint, column after the inner hint';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(indexHint(k = 7), v = 8));
SELECT 'alias, MergeTree';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(a = 7));
SELECT 'alias, Memory';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only_mem) QUALIFY indexHint(a = 7));
SELECT 'alias, Distributed';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM d_hint_only QUALIFY indexHint(a = 7));
SELECT 'alias with an indexHint body, WHERE, MergeTree';
SELECT
    (SELECT count() FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) WHERE h))
        = 2 * (SELECT count() FROM (SELECT count() OVER () AS c FROM remote('127.0.0.1', currentDatabase(), t_hint_only) WHERE h))
    AND (SELECT count() FROM (SELECT count() OVER () AS c FROM remote('127.0.0.1', currentDatabase(), t_hint_only) WHERE h)) > 0;
SELECT 'alias with an indexHint body, WHERE, Memory';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only_mem) WHERE h);
SELECT 'alias with an indexHint body, local PREWHERE';
SELECT (SELECT count() FROM t_hint_only PREWHERE h AND v > 5) = (SELECT count() FROM t_hint_only PREWHERE indexHint(k = 7) AND v > 5);
SELECT 'serialize_query_plan';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(k = 7)) SETTINGS serialize_query_plan = 1;
SELECT 'prefer_localhost_replica';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(k = 7)) SETTINGS prefer_localhost_replica = 1;
SELECT 'fuzzer shape';
SELECT count() IGNORE NULLS FROM (SELECT count() OVER (GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s, k FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(7 <=> k)) GROUP BY ALL WITH TOTALS;

SELECT 'control: column also selected';
SELECT count(), max(k), max(c) FROM (SELECT k, count() OVER () AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) QUALIFY indexHint(k = 7));
SELECT 'control: single shard';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM remote('127.0.0.1', currentDatabase(), t_hint_only) QUALIFY indexHint(k = 7));
SELECT 'control: local table';
SELECT count(), max(c) FROM (SELECT count() OVER () AS c FROM t_hint_only QUALIFY indexHint(k = 7));
SELECT 'control: having';
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_hint_only) GROUP BY k HAVING indexHint(k = 7));

DROP TABLE d_hint_only;
DROP TABLE t_hint_only;
DROP TABLE t_hint_only_mem;
