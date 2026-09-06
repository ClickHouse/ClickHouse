-- A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in that materializes
-- the real storage on first access, and the catalog keeps the stand-in afterwards. Everything that reads
-- the storage object itself - the `system.parts` family (which recognizes an engine by downcasting), the
-- `DELETE`/mutation capability predicates, and `BACKUP` - used to see the stand-in instead of the table.

DROP DATABASE IF EXISTS db_05078_lazy;
CREATE DATABASE db_05078_lazy ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE db_05078_lazy.mt (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO db_05078_lazy.mt SELECT number FROM numbers(100);

-- Reloading the database installs the stand-in, as a server restart does.
DETACH DATABASE db_05078_lazy;
ATTACH DATABASE db_05078_lazy;
SELECT engine FROM system.tables WHERE database = 'db_05078_lazy' AND name = 'mt';

-- `BACKUP` of a table that has not been accessed since must not lose its data.
BACKUP TABLE db_05078_lazy.mt TO Memory('backup_05078') FORMAT Null;
DROP TABLE db_05078_lazy.mt SYNC;
RESTORE TABLE db_05078_lazy.mt FROM Memory('backup_05078') FORMAT Null;
SELECT count() FROM db_05078_lazy.mt;

DETACH DATABASE db_05078_lazy;
ATTACH DATABASE db_05078_lazy;

-- A query materializes the table; the system tables must see it from then on.
SELECT count() FROM db_05078_lazy.mt;
SELECT count() FROM system.parts WHERE database = 'db_05078_lazy' AND table = 'mt' AND active;
SELECT count() FROM system.parts_columns WHERE database = 'db_05078_lazy' AND table = 'mt' AND active AND column = 'a';

-- `DELETE` and mutations are accepted.
DELETE FROM db_05078_lazy.mt WHERE a = 1;
ALTER TABLE db_05078_lazy.mt DELETE WHERE a = 2 SETTINGS mutations_sync = 2;
SELECT count() FROM db_05078_lazy.mt;

DROP DATABASE db_05078_lazy;
