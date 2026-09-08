-- Reading from a Join-engine table (StorageJoin) with a single LowCardinality key, and joinGet on it,
-- must work. The dictionary-aware map is read back through the limited variant set, the same way as
-- the non-LowCardinality maps (they are physically identical and store key values). Verify the
-- contents and joinGet results match the equivalent plain-keyed Join table.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS sj_lc_str;
DROP TABLE IF EXISTS sj_pl_str;
CREATE TABLE sj_lc_str (k LowCardinality(String), v UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE sj_pl_str (k String, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO sj_lc_str SELECT toString(number), number FROM numbers(100);
INSERT INTO sj_pl_str SELECT toString(number), number FROM numbers(100);
SELECT 'str_select',  (SELECT groupBitXor(cityHash64(toString(k), v)) FROM sj_lc_str) = (SELECT groupBitXor(cityHash64(toString(k), v)) FROM sj_pl_str);
SELECT 'str_filtered', (SELECT v FROM sj_lc_str WHERE k = '42') = (SELECT v FROM sj_pl_str WHERE k = '42');
SELECT 'str_joinget',  joinGet('sj_lc_str', 'v', '42') = joinGet('sj_pl_str', 'v', '42');

DROP TABLE IF EXISTS sj_lc_u32;
DROP TABLE IF EXISTS sj_pl_u32;
CREATE TABLE sj_lc_u32 (k LowCardinality(UInt32), v UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE sj_pl_u32 (k UInt32, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO sj_lc_u32 SELECT number, number * 10 FROM numbers(100);
INSERT INTO sj_pl_u32 SELECT number, number * 10 FROM numbers(100);
SELECT 'u32_select',  (SELECT groupBitXor(cityHash64(k, v)) FROM sj_lc_u32) = (SELECT groupBitXor(cityHash64(k, v)) FROM sj_pl_u32);
SELECT 'u32_joinget', joinGet('sj_lc_u32', 'v', toUInt32(42)) = joinGet('sj_pl_u32', 'v', toUInt32(42));

DROP TABLE sj_lc_str;
DROP TABLE sj_pl_str;
DROP TABLE sj_lc_u32;
DROP TABLE sj_pl_u32;
