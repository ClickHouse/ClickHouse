-- An identifier as the first argument of `remote` names either a named collection or a cluster
-- from the server configuration. In the cluster-name form the user and the password are ordinary
-- positional arguments, so the secret arguments finder has to run its positional walk for it too.

-- These run: test_shard_localhost is a configured cluster, so the positional credentials are
-- parsed and then unused.
SELECT 1 FROM remote(test_shard_localhost, system, one, 'usr', 'SECRETPW1');
SELECT 1 FROM remote(test_shard_localhost, system.one, 'usr', 'SECRETPW2');

-- An unknown cluster is reported only after the whole argument list has been parsed and logged.
SELECT 1 FROM remote(cluster_that_does_not_exist_05237, system, one, 'usr', 'SECRETPW3'); -- { serverError CLUSTER_DOESNT_EXIST }
SELECT 1 FROM remoteSecure(cluster_that_does_not_exist_05237, system, one, 'usr', 'SECRETPW4'); -- { serverError CLUSTER_DOESNT_EXIST }

-- The addresses form, for comparison: `remote('')` is rejected in parseAddress, also after the
-- whole argument list has been parsed.
SELECT 1 FROM remote('', 'db', 't', 'usr', 'SECRETPW5'); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM remote('', db.t, 'usr', 'SECRETPW6'); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM remote('', numbers(10), 'usr', 'SECRETPW7'); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM remote('', 'db', 't', 'usr', 'SECRETPW8', rand()); -- { serverError BAD_ARGUMENTS }

-- Not credentials, and they have to stay readable: a user name with no password after it, a
-- sharding key spelled as an equality whose left operand happens to be called `password`, and a
-- SETTINGS clause.
SELECT 1 FROM remote(test_shard_localhost, system, one, 'usr');
SELECT 1 FROM remote('', 'db', 't', 'usr', password = 'NOT_A_CREDENTIAL'); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM remote('', 'db', 't', 'usr', rand(), SETTINGS skip_unavailable_shards = 1); -- { serverError BAD_ARGUMENTS }

-- The analyzer keeps its own finder over the query tree, and `EXPLAIN QUERY TREE` renders through
-- it, so the same statement is masked on that path too.
SELECT countIf(explain LIKE '%[HIDDEN]%') = 1, countIf(explain LIKE '%SECRETPW9%') = 0, countIf(explain LIKE '%usr%') = 1
FROM (EXPLAIN QUERY TREE SELECT 1 FROM remote(test_shard_localhost, system, one, 'usr', 'SECRETPW9'));

SYSTEM FLUSH LOGS query_log;

-- `system.query_log` holds the same masked text as `system.text_log` and the server log. The two
-- queries below exclude themselves through `system.query_log` and `EXPLAIN`, and they must not
-- filter on the secret: once it is masked, a secret-keyed filter matches nothing and passes
-- vacuously.
SELECT DISTINCT query FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND query LIKE '%FROM remote%'
  AND query NOT LIKE '%system.query_log%'
  AND query NOT LIKE '%EXPLAIN%'
  AND query LIKE '%[HIDDEN]%'
ORDER BY query;

SELECT
    uniqExact(query) = 11,
    countIf(query LIKE '%SECRETPW%') = 0,
    uniqExactIf(query, query LIKE '%[HIDDEN]%') = 8,
    uniqExactIf(query, query LIKE '%NOT_A_CREDENTIAL%') = 1,
    uniqExactIf(query, query LIKE '%skip_unavailable_shards%') = 1
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND query LIKE '%FROM remote%'
  AND query NOT LIKE '%system.query_log%'
  AND query NOT LIKE '%EXPLAIN%';
