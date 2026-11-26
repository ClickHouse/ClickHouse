-- Tags: no-fasttest
-- no-fasttest: the S3 table engine is not available in the fast test build.

-- role_session_name inside extra_credentials(...) can act as a shared secret: a role's trust policy
-- can require a specific value through the sts:RoleSessionName condition, and the ClickHouse Cloud
-- guide documents exactly this use. It must therefore be masked like external_id, while role_arn is
-- a non-secret identifier that stays visible. Every secret value below is tagged so the final
-- assertion can prove none of them leaks. role_session_name used to be logged in plaintext.

-- Engine form: SHOW CREATE hides role_session_name and external_id and keeps role_arn.
DROP TABLE IF EXISTS t_05255;
CREATE TABLE t_05255 (x UInt8)
ENGINE = S3('http://localhost:11111/test/05255', 'ak', 'SEKRIT_SAK',
            extra_credentials(role_arn = 'visible_role_arn', role_session_name = 'SEKRIT_RSN', external_id = 'SEKRIT_EID'),
            format = 'TSV');
SHOW CREATE TABLE t_05255 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_05255;

-- The forms below all fail at analysis (empty host / missing collection) before any network access,
-- and are logged with the secret replaced. Each carries a unique marker checked by the final assertion.

-- Explicit-url function form in the shape users write it: no access key, the role alone authenticates.
SELECT * FROM s3('url_rsn', 'Parquet',
                 extra_credentials(role_arn = 'arn:aws:iam::123456789012:role/visible_role', role_session_name = 'SEKRIT_RSN')); -- { serverError BAD_ARGUMENTS }

-- Together with the other two keys of the triple, at any position inside the map.
SELECT * FROM s3('url_rsn_triple', 'ak', 'SEKRIT_SAK',
                 extra_credentials(role_session_name = 'SEKRIT_RSNFIRST', role_arn = 'visible_role_arn', external_id = 'SEKRIT_EID'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- The parser evaluates an identifier value as a literal, so an identifier carries the secret too.
SELECT * FROM s3('url_rsn_ident', 'ak', 'SEKRIT_SAK',
                 extra_credentials(role_arn = visible_role_arn, role_session_name = SEKRIT_IDRSN),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- Named-collection form: role_session_name as a named override of the collection.
SELECT * FROM s3(nc_05255_missing, role_session_name = 'SEKRIT_NCRSN',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- Named-collection form with the nested map.
SELECT * FROM s3(nc_05255_missing, extra_credentials(role_arn = 'visible_role_arn', role_session_name = 'SEKRIT_NCMAPRSN'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- BACKUP ... TO S3: the explicit-url locator with the nested map, and the named-collection locator
-- with a named override.
BACKUP TABLE nonexistent_05255 TO S3('url_bkp_rsn', 'ak', 'SEKRIT_SAK',
                 extra_credentials(role_arn = 'visible_role_arn', role_session_name = 'SEKRIT_BKPRSN')); -- { serverError BAD_ARGUMENTS }
BACKUP TABLE nonexistent_05255 TO S3(nc_bkp_05255_missing,
                 role_session_name = 'SEKRIT_BKPNCRSN'); -- { serverError BAD_ARGUMENTS }

-- The Backup database engine reconstructs the nested S3 destination.
CREATE DATABASE db_05255_rsn ENGINE = Backup('', S3('url_dbrsn', 'ak', 'SEKRIT_SAK',
                 extra_credentials(role_arn = 'visible_role_arn', role_session_name = 'SEKRIT_DBRSN'))); -- { serverError BAD_ARGUMENTS }

-- The query-tree surface (EXPLAIN QUERY TREE) must hide the same value while keeping role_arn.
-- run_passes = 0 keeps the table function unresolved, so the collection and the storage are not touched.
SET enable_analyzer = 1;
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('http://localhost:11111/test/05255qt', 'Parquet', extra_credentials(role_arn = 'visible_role_arn', role_session_name = 'SEKRIT_QTRSN'));

SYSTEM FLUSH LOGS query_log;

-- The exact logged text of every query above, in execution order: role_session_name must appear as
-- '[HIDDEN]' while role_arn and every other non-secret part stay visible verbatim. Each query has
-- exactly one terminal event: QueryFinish for the successful ones, an exception event for the
-- rejected ones.
SELECT query
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND query_kind != 'Set' -- sent by the test harness, not by this test
  AND query NOT ILIKE 'SYSTEM FLUSH%' -- its own terminal event races with the flush it performs
  AND query_id = initial_query_id -- only the statements issued here: a Replicated database logs
                                  -- each DDL again from the replay worker, which inherits the
                                  -- initiator's initial_query_id but gets a fresh query_id
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
ORDER BY event_time_microseconds;

-- Assert the masking property over every row this test produced, replay rows included.
-- count() > 0 keeps an empty row set from passing vacuously.
SELECT count() > 0, countIf(query LIKE '%SEKRIT%')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
