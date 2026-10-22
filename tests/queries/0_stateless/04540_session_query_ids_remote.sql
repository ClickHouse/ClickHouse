-- Tags: shard

-- Secondary queries that distributed queries run on remote shards are executed with sessions
-- of pooled inter-server connections shared between initiators, so they are not recorded.
SELECT count() FROM remote('127.0.0.2', system.session_query_ids);

-- Only the initiating queries appear in this session: the remote read above and this query.
SELECT count() FROM system.session_query_ids;
