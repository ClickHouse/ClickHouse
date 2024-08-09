DROP TABLE IF EXISTS kv;

CREATE TABLE kv (k UInt32, v UInt32) ENGINE Join(Any, Left, k);
INSERT INTO kv VALUES (1, 2);
INSERT INTO kv VALUES (1, 3);
SELECT joinGet('kv', 'v', toUInt32(1));
CREATE TABLE kv_overwrite (k UInt32, v UInt32) ENGINE Join(Any, Left, k) SETTINGS join_any_take_last_row = 1;
INSERT INTO kv_overwrite VALUES (1, 2);
INSERT INTO kv_overwrite VALUES (1, 3);
SELECT joinGet('kv_overwrite', 'v', toUInt32(1));

DROP TABLE kv;
DROP TABLE kv_overwrite;
