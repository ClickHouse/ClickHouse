DROP TABLE IF EXISTS t_ttl_move_if_exists;

CREATE TABLE t_ttl_move_if_exists (d DateTime, a UInt32)
ENGINE = MergeTree ORDER BY tuple()
TTL d TO DISK IF EXISTS 'non_existing_disk';

SHOW CREATE TABLE t_ttl_move_if_exists;

DROP TABLE IF EXISTS t_ttl_move_if_exists;
