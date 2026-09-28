-- Tags: no-fasttest
-- no-fasttest: the S3 backup engine is not available in the fast test build.

-- A `Backup` database hides the secrets of its `S3(...)` destination in the logged query the same way
-- `BACKUP ... TO` hides them for the same destination. A `headers(...)` map takes no positional slot and
-- keeps its keys with the values hidden, and a function in the last position takes no positional slot
-- either. Every statement is rejected, but only after it is logged, so any other argument that is not a
-- literal (a function or an identifier in another position) is hidden.

CREATE DATABASE db_05291_hdr2 ENGINE = Backup('', S3('url_dbhdr2', 'SEKRIT_DBHDR2',
    headers('X-Auth' = 'SEKRIT_DBHDR2V'))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE DATABASE db_05291_hdr3 ENGINE = Backup('', S3('url_dbhdr3', 'SEKRIT_DBHDR3',
    headers('X-Auth' = 'SEKRIT_DBHDR3V'), extra_credentials(role_arn = 'visible_05291_role'))); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE db_05291_hdr4 ENGINE = Backup('', S3(nc_05291_missing,
    headers('X-Auth' = 'SEKRIT_DBHDR4V'), 'visible_05291_dir')); -- { serverError BAD_ARGUMENTS }
BACKUP TABLE nonexistent_05291 TO S3('url_bkpfn', 'SEKRIT_BKPFN', concat('SEKRIT_BKPFNX', 'x')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE DATABASE db_05291_fn ENGINE = Backup('', S3('url_dbfn', 'SEKRIT_DBFN',
    concat('SEKRIT_DBFNX', 'x'))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
BACKUP TABLE nonexistent_05291 TO S3('url_bkpmid', concat('SEKRIT_BKPMID', 'x'), 'SEKRIT_BKPMIDS'); -- { serverError BAD_ARGUMENTS }
BACKUP TABLE nonexistent_05291 TO S3('url_bkpid', SEKRIT_BKPID, 'SEKRIT_BKPIDS'); -- { serverError BAD_ARGUMENTS }
BACKUP TABLE nonexistent_05291 TO S3(nc_05291_missing, concat('SEKRIT_BKPNC', 'x'),
    headers('X-Auth' = 'SEKRIT_BKPNCV')); -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

-- The logged text of every statement above, in execution order.
SELECT query
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND query_kind != 'Set' -- sent by the test harness, not by this test
  AND query NOT ILIKE 'SYSTEM FLUSH%' -- its own terminal event races with the flush it performs
  AND query_id = initial_query_id -- a Replicated database logs each DDL again from its replay worker
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
ORDER BY event_time_microseconds;

-- No secret in any row this test produced, replay rows included. count() > 0 keeps an empty row set
-- from passing.
SELECT count() > 0, countIf(query LIKE '%SEKRIT%')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
