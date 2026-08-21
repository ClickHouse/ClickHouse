-- An async INSERT into a Distributed/Remote table spools blocks into a directory named after the
-- destination address, so a name over NAME_MAX (255) must be a user error, not a bare std::exception.
-- The port is spelled out in every address so the name length does not depend on the server's config.

DROP TABLE IF EXISTS target_04725;
CREATE TABLE target_04725 (x UInt64) ENGINE = MergeTree ORDER BY x;

-- A name of exactly 255 bytes ('default' + '@' + host + ':9000') is still accepted.
CREATE TABLE at_limit_04725 (x UInt64) ENGINE = Remote('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:9000', currentDatabase(), target_04725);
SYSTEM STOP DISTRIBUTED SENDS at_limit_04725;
INSERT INTO at_limit_04725 SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0, use_compact_format_in_distributed_parts_names = 0 VALUES (1);
SELECT 'at_limit_queued', data_files > 0 FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'at_limit_04725';

-- One byte over the limit is rejected.
CREATE TABLE over_limit_04725 (x UInt64) ENGINE = Remote('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:9000', currentDatabase(), target_04725);
SYSTEM STOP DISTRIBUTED SENDS over_limit_04725;
INSERT INTO over_limit_04725 SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0, use_compact_format_in_distributed_parts_names = 0 VALUES (1); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A shard whose FIRST destination is short and a LATER one too long is rejected too, and the short
-- one gets no queue directory.
CREATE TABLE multi_04725 (x UInt64) ENGINE = Remote('127.0.0.1:9000|bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:9000', currentDatabase(), target_04725);
SYSTEM STOP DISTRIBUTED SENDS multi_04725;
INSERT INTO multi_04725 SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0, use_compact_format_in_distributed_parts_names = 0 VALUES (1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'multi_no_queue', count() FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'multi_04725';

-- The directory name embeds the password, so the message must not disclose it.
CREATE TABLE with_password_04725 (x UInt64) ENGINE = Remote('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:9000', currentDatabase(), target_04725, 'default', 'secret_04725');
SYSTEM STOP DISTRIBUTED SENDS with_password_04725;
INSERT INTO with_password_04725 SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0, use_compact_format_in_distributed_parts_names = 0 VALUES (1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SYSTEM FLUSH LOGS query_log;
SELECT 'password_not_leaked', countIf(exception LIKE '%secret_04725%') FROM system.query_log
WHERE current_database = currentDatabase() AND exception_code = 69;
SELECT 'message_names_scope', countIf(exception LIKE '%over_limit_04725%' AND exception LIKE '%is 255%') FROM system.query_log
WHERE current_database = currentDatabase() AND exception_code = 69 AND query LIKE '%over_limit_04725%';

-- The compact format builds bounded 'shard<N>_replica<M>' names, so it is unaffected.
INSERT INTO over_limit_04725 SETTINGS distributed_foreground_insert = 0, prefer_localhost_replica = 0, use_compact_format_in_distributed_parts_names = 1 VALUES (1);
SELECT 'compact_format_ok', data_files > 0 FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'over_limit_04725';

DROP TABLE with_password_04725;
DROP TABLE multi_04725;
DROP TABLE over_limit_04725;
DROP TABLE at_limit_04725;
DROP TABLE target_04725;
