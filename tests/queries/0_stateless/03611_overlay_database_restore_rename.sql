-- Tags: no-parallel
-- RESTORE ... AS rewrites the source database names of an Overlay facade through the renaming map,
-- so a facade restored under a new name (with its sources also restored under new names) points at
-- the restored sources rather than the originals.
-- { echo }
DROP DATABASE IF EXISTS db_ov_src;
DROP DATABASE IF EXISTS db_ov_face;
DROP DATABASE IF EXISTS db_ov_src2;
DROP DATABASE IF EXISTS db_ov_face2;

CREATE DATABASE db_ov_src ENGINE = Atomic;
CREATE TABLE db_ov_src.t (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO db_ov_src.t VALUES (1), (2), (3);

CREATE DATABASE db_ov_face ENGINE = Overlay('db_ov_src');
SELECT * FROM db_ov_face.t ORDER BY id;

-- Back up the source database and its facade together.
BACKUP DATABASE db_ov_src, DATABASE db_ov_face TO Memory('03611_overlay_restore_rename') FORMAT Null;

-- Restore both under new names: the restored facade must point at the restored source (db_ov_src2),
-- not at the original db_ov_src.
RESTORE DATABASE db_ov_src AS db_ov_src2, DATABASE db_ov_face AS db_ov_face2 FROM Memory('03611_overlay_restore_rename') FORMAT Null;

SHOW CREATE DATABASE db_ov_face2;
SELECT * FROM db_ov_face2.t ORDER BY id;

-- Mutating the original source does not affect the restored facade: it reads from db_ov_src2.
INSERT INTO db_ov_src.t VALUES (4);
SELECT count() FROM db_ov_face2.t;

DROP DATABASE db_ov_face2;
DROP DATABASE db_ov_src2;
DROP DATABASE db_ov_face;
DROP DATABASE db_ov_src;
