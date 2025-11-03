SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET log_queries = 1;
SET log_query_settings = 1;
SET mutations_sync = 2;

-- A) Base (Nullable): load then hit
DROP TABLE IF EXISTS sc_core SYNC;

CREATE TABLE sc_core (k UInt32, v Nullable(Float64))
ENGINE=MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 1;

INSERT INTO sc_core
SELECT number, if(number%20=0, NULL, toFloat64(rand())/4294967296.0)
FROM numbers(200000);

ALTER TABLE sc_core ADD STATISTICS v TYPE TDigest;
ALTER TABLE sc_core MATERIALIZE STATISTICS ALL;

SELECT count() FROM sc_core WHERE v > 0.99
SETTINGS use_statistics_cache = 0, log_comment='core-load' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds'] > 0, 'yes', 'no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='core-load' LIMIT 1;

SELECT sleep(1);

SELECT count() FROM sc_core WHERE v > 0.99
SETTINGS use_statistics_cache = 1, log_comment='core-hit' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds'] = 0, 'yes', 'no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='core-hit' LIMIT 1;

-- B) No-usage guard (no stats used by SUM)
DROP TABLE IF EXISTS sc_unused SYNC;

CREATE TABLE sc_unused (k UInt64, val UInt64)
ENGINE=MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 1;

INSERT INTO sc_unused SELECT number, number%100 FROM numbers(100000);

ALTER TABLE sc_unused ADD STATISTICS val TYPE MinMax;
ALTER TABLE sc_unused MATERIALIZE STATISTICS ALL;

SELECT sum(val) FROM sc_unused
SETTINGS use_statistics_cache = 0, log_comment='nouse-agg' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0, 'yes', 'no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='nouse-agg' LIMIT 1;

-- C) OPTIMIZE FINAL (no data change -> still hit)
OPTIMIZE TABLE sc_core FINAL;

SELECT sleep(1);

SELECT count() FROM sc_core WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='core-after-opt' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='core-after-opt' LIMIT 1;

-- D) TDigest / CountMin / MinMax with NULLs
DROP TABLE IF EXISTS st_tdg SYNC;

CREATE TABLE st_tdg (k UInt32, val Nullable(Float64)) ENGINE=MergeTree ORDER BY k;

INSERT INTO st_tdg SELECT number, if(number%23=0,NULL,toFloat64(rand())/4294967296.0) FROM numbers(200000);

ALTER TABLE st_tdg ADD STATISTICS val TYPE TDigest;
ALTER TABLE st_tdg MATERIALIZE STATISTICS ALL;

SELECT count() FROM st_tdg WHERE val > 0.99
SETTINGS use_statistics_cache=0, log_comment='tdg-load' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='tdg-load' LIMIT 1;

DROP TABLE IF EXISTS st_cm SYNC;

CREATE TABLE st_cm (k UInt32, cat Nullable(String)) ENGINE=MergeTree ORDER BY k;

INSERT INTO st_cm
SELECT number,
       if(number%17=0,NULL, if(number%4=0,'PROMO', concat('X', toString(number%1000))))
FROM numbers(200000);

ALTER TABLE st_cm ADD STATISTICS cat TYPE CountMin;
ALTER TABLE st_cm MATERIALIZE STATISTICS ALL;

SELECT count() FROM st_cm WHERE cat='PROMO'
SETTINGS use_statistics_cache=0, log_comment='cm-load' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='cm-load' LIMIT 1;

DROP TABLE IF EXISTS st_mm SYNC;

CREATE TABLE st_mm (k UInt32, x Nullable(UInt32)) ENGINE=MergeTree ORDER BY k;

INSERT INTO st_mm SELECT number, if(number%29=0,NULL, number%1000000) FROM numbers(300000);

ALTER TABLE st_mm ADD STATISTICS x TYPE MinMax;
ALTER TABLE st_mm MATERIALIZE STATISTICS ALL;

SELECT count() FROM st_mm WHERE x BETWEEN 900000 AND 950000
SETTINGS use_statistics_cache=0, log_comment='mm-load' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='mm-load' LIMIT 1;

-- E) LowCardinality coverage - https://github.com/ClickHouse/ClickHouse/issues/87886
DROP TABLE IF EXISTS st_cm_lc SYNC;

CREATE TABLE st_cm_lc
(
    k   UInt32,
    cat LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS refresh_statistics_interval = 1;

INSERT INTO st_cm_lc
SELECT number,
       if(number % 4 = 0, 'PROMO', concat('X', toString(number % 1000)))
FROM numbers(200000);

ALTER TABLE st_cm_lc ADD STATISTICS cat TYPE CountMin;
ALTER TABLE st_cm_lc MATERIALIZE STATISTICS ALL;

-- Force load (bypass cache)
SELECT count() FROM st_cm_lc WHERE cat = 'PROMO'
SETTINGS use_statistics_cache = 0, log_comment = 'cm-lc-load'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds'] > 0, 'yes', 'no')
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'cm-lc-load' LIMIT 1;

-- Hit with cache
SELECT sleep(1);

SELECT count() FROM st_cm_lc WHERE cat = 'PROMO'
SETTINGS use_statistics_cache = 1, log_comment = 'cm-lc-hit'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds'] = 0, 'yes', 'no')
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'cm-lc-hit' LIMIT 1;

DROP TABLE IF EXISTS sj_a SYNC; 
DROP TABLE IF EXISTS sj_b SYNC;

CREATE TABLE sj_a (id UInt32, p UInt8) ENGINE=MergeTree ORDER BY id;
CREATE TABLE sj_b (id UInt32, t LowCardinality(String)) ENGINE=MergeTree ORDER BY id;

INSERT INTO sj_a SELECT number, number%2 FROM numbers(120000);
INSERT INTO sj_b SELECT number, if(number%5=0,'PROMO','OTHER') FROM numbers(120000);

ALTER TABLE sj_a ADD STATISTICS id TYPE Uniq;
ALTER TABLE sj_b ADD STATISTICS id TYPE Uniq;

ALTER TABLE sj_a MATERIALIZE STATISTICS ALL;
ALTER TABLE sj_b MATERIALIZE STATISTICS ALL;

SELECT count()
FROM sj_a a, sj_b b
WHERE a.id=b.id AND b.t='PROMO'
SETTINGS use_statistics_cache=0, query_plan_optimize_join_order_limit=10, log_comment='join-load'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='join-load' LIMIT 1;

DROP TABLE IF EXISTS sj_ac SYNC;
DROP TABLE IF EXISTS sj_bc SYNC;

CREATE TABLE sj_ac (id UInt32, p UInt8) ENGINE=MergeTree ORDER BY id
SETTINGS refresh_statistics_interval=1;

CREATE TABLE sj_bc (id UInt32, t LowCardinality(String)) ENGINE=MergeTree ORDER BY id
SETTINGS refresh_statistics_interval=1;

INSERT INTO sj_ac SELECT number, number%2 FROM numbers(120000);
INSERT INTO sj_bc SELECT number, if(number%5=0,'PROMO','OTHER') FROM numbers(120000);

ALTER TABLE sj_ac ADD STATISTICS id TYPE Uniq; 
ALTER TABLE sj_bc ADD STATISTICS id TYPE Uniq;

ALTER TABLE sj_ac MATERIALIZE STATISTICS ALL; 
ALTER TABLE sj_bc MATERIALIZE STATISTICS ALL;

SELECT sleep(1);

SELECT count()
FROM sj_ac a, sj_bc b
WHERE a.id=b.id AND b.t='PROMO'
SETTINGS use_statistics_cache=1, query_plan_optimize_join_order_limit=10, log_comment='join-hit'
FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='join-hit' LIMIT 1;

-- F) auto_statistics_types (with NULLs)
DROP TABLE IF EXISTS sa_auto SYNC;

CREATE TABLE sa_auto
(
  k UInt32,
  val Nullable(Float64),
  cat Nullable(String),
  r UInt32
)
ENGINE=MergeTree ORDER BY k
SETTINGS refresh_statistics_interval=1,
         auto_statistics_types='tdigest,countmin,minmax,uniq';

INSERT INTO sa_auto
SELECT number,
       if(number%13=0,NULL,toFloat64(rand())/4294967296.0),
       if(number%11=0,NULL, if(number%7=0,'PROMO', concat('G',toString(number%500)))),
       number%1000000
FROM numbers(200000);

ALTER TABLE sa_auto MATERIALIZE STATISTICS ALL;

SELECT sleep(1);

SELECT count() FROM sa_auto WHERE val>0.99 SETTINGS use_statistics_cache=1, log_comment='auto-td' FORMAT Null;
SELECT count() FROM sa_auto WHERE cat='PROMO'  SETTINGS use_statistics_cache=1, log_comment='auto-cm' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT
    if(countIf(log_comment='auto-td' AND ProfileEvents['LoadedStatisticsMicroseconds']=0) >= 1, 'yes', 'no'),
    if(countIf(log_comment='auto-cm' AND ProfileEvents['LoadedStatisticsMicroseconds']=0) >= 1, 'yes', 'no')
FROM system.query_log
WHERE type='QueryFinish' AND log_comment IN ('auto-td','auto-cm');

-- G) ALTER interval persists, takes effect after DETACH/ATTACH
DROP TABLE IF EXISTS sc_alter SYNC;
CREATE TABLE sc_alter (k UInt32, v Nullable(Float64))
ENGINE=MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 0;

INSERT INTO sc_alter
SELECT number, if(number%20=0,NULL,toFloat64(rand())/4294967296.0)
FROM numbers(150000);

ALTER TABLE sc_alter ADD STATISTICS v TYPE TDigest;
ALTER TABLE sc_alter MATERIALIZE STATISTICS ALL;

SELECT count() FROM sc_alter WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='alter-pre' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='alter-pre' LIMIT 1;

ALTER TABLE sc_alter MODIFY SETTING refresh_statistics_interval = 1;
SELECT
    if(positionCaseInsensitive(coalesce(create_table_query,''), 'refresh_statistics_interval = 1') > 0, 'yes', 'no')
FROM system.tables WHERE database=currentDatabase() AND name='sc_alter';

SELECT count() FROM sc_alter WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='alter-immediate' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='alter-immediate' LIMIT 1;

DETACH TABLE sc_alter;
ATTACH TABLE sc_alter;

SELECT sleep(1);

SELECT count() FROM sc_alter WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='alter-post' FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='alter-post' LIMIT 1;

-- H) TRUNCATE
DROP TABLE IF EXISTS sc_trunc SYNC;

CREATE TABLE sc_trunc (k UInt32, v Float64)
ENGINE=MergeTree ORDER BY k
SETTINGS refresh_statistics_interval = 1;

INSERT INTO sc_trunc SELECT number, toFloat64(rand())/4294967296.0 FROM numbers(150000);

ALTER TABLE sc_trunc ADD STATISTICS v TYPE TDigest;
ALTER TABLE sc_trunc MATERIALIZE STATISTICS ALL;

SELECT sleep(1);

SELECT count() FROM sc_trunc WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='trunc-warm' FORMAT Null;

SYSTEM FLUSH LOGS;

SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0,'yes','no')
FROM system.query_log WHERE type='QueryFinish' AND log_comment='trunc-warm' LIMIT 1;

TRUNCATE TABLE sc_trunc;

-- Immediately after TRUNCATE: no parts -> no stats requested -> no load
SELECT count() FROM sc_trunc WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='trunc-after' FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0,'yes','no')
FROM system.query_log
WHERE type='QueryFinish' AND log_comment='trunc-after' LIMIT 1;

-- Insert new data -> first query should load once
INSERT INTO sc_trunc SELECT number, toFloat64(rand())/4294967296.0 FROM numbers(150000);

SELECT count() FROM sc_trunc WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='trunc-load' FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']>0,'yes','no')
FROM system.query_log
WHERE type='QueryFinish' AND log_comment='trunc-load' LIMIT 1;

-- After interval tick -> hit (no per-query load)
SELECT sleep(1);

SELECT count() FROM sc_trunc WHERE v > 0.99
SETTINGS use_statistics_cache=1, log_comment='trunc-hit' FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT if(ProfileEvents['LoadedStatisticsMicroseconds']=0,'yes','no')
FROM system.query_log
WHERE type='QueryFinish' AND log_comment='trunc-hit' LIMIT 1;
