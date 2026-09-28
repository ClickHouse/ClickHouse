-- `ExecutablePool` fills in `max_command_execution_time` itself when its definition does not state it - 10 seconds,
-- capped by the server's `max_execution_time` - so the value decides whether the setting reads as changed, as for
-- what `Distributed` copies from the server. Stated in the definition, it is the definition's.

DROP TABLE IF EXISTS pool;
DROP TABLE IF EXISTS pool_stated;

CREATE TABLE pool (x UInt64) ENGINE = ExecutablePool('nonexistent.sh', 'TabSeparated');
CREATE TABLE pool_stated (x UInt64) ENGINE = ExecutablePool('nonexistent.sh', 'TabSeparated')
    SETTINGS max_command_execution_time = 5;

SELECT '-- filled in: changed only where the value is not the default';
SELECT name, changed = (value != `default`), source = if(value = `default`, 'default', 'other')
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'pool' AND name = 'max_command_execution_time';

SELECT '-- stated';
SELECT name, value, changed, source
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'pool_stated' AND name = 'max_command_execution_time';

DROP TABLE pool;
DROP TABLE pool_stated;
