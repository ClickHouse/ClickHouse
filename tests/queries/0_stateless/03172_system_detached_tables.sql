-- Tags: no-parallel

SELECT 'database atomic tests';
DROP DATABASE IF EXISTS test03172_system_detached_tables;
CREATE DATABASE IF NOT EXISTS test03172_system_detached_tables ENGINE=Atomic;

CREATE TABLE test03172_system_detached_tables.test_table (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables WHERE database='test03172_system_detached_tables';

DETACH TABLE test03172_system_detached_tables.test_table;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='test03172_system_detached_tables';

ATTACH TABLE test03172_system_detached_tables.test_table;

CREATE TABLE test03172_system_detached_tables.test_table_perm (n Int64) ENGINE=MergeTree ORDER BY n;
SELECT * FROM system.detached_tables WHERE database='test03172_system_detached_tables';

DETACH TABLE test03172_system_detached_tables.test_table_perm PERMANENTLY;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='test03172_system_detached_tables';

DETACH TABLE test03172_system_detached_tables.test_table SYNC;
SELECT database, table, is_permanently FROM system.detached_tables WHERE database='test03172_system_detached_tables';

SELECT database, table, is_permanently FROM system.detached_tables WHERE database='test03172_system_detached_tables' AND table='test_table';

DROP DATABASE test03172_system_detached_tables SYNC;

SELECT '-----------------------';
SELECT 'database lazy tests';

DROP DATABASE IF EXISTS test03172_system_detached_tables_lazy;
CREATE DATABASE test03172_system_detached_tables_lazy Engine=Lazy(10);

CREATE TABLE test03172_system_detached_tables_lazy.test_table (number UInt64) engine=Log;
INSERT INTO test03172_system_detached_tables_lazy.test_table SELECT * FROM numbers(100);
DETACH TABLE test03172_system_detached_tables_lazy.test_table;

CREATE TABLE test03172_system_detached_tables_lazy.test_table_perm (number UInt64) engine=Log;
INSERT INTO test03172_system_detached_tables_lazy.test_table_perm SELECT * FROM numbers(100);
DETACH table test03172_system_detached_tables_lazy.test_table_perm PERMANENTLY;

SELECT 'before attach', database, table, is_permanently FROM system.detached_tables WHERE database='test03172_system_detached_tables_lazy';

ATTACH TABLE test03172_system_detached_tables_lazy.test_table;
ATTACH TABLE test03172_system_detached_tables_lazy.test_table_perm;

SELECT 'after attach', database, table, is_permanently FROM system.detached_tables WHERE database='test03172_system_detached_tables_lazy';

SELECT 'DROP TABLE';
DROP TABLE  test03172_system_detached_tables_lazy.test_table SYNC;
DROP TABLE  test03172_system_detached_tables_lazy.test_table_perm SYNC;

DROP DATABASE test03172_system_detached_tables_lazy SYNC;
