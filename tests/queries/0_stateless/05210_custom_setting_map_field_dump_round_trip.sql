-- Tags: shard

-- A custom setting travels on the native protocol as the text dump of its Field, so a map valued
-- one is sent as Map_(Tuple_('k', 'v')) and has to be restored by Field::restoreFromDump. The
-- session arms below fail on the query that follows the SET, not on the SET itself; the remote arm
-- fails on the shard, which reads the same dump out of the query's settings.

SET SQL_05210_map = {'k':'v'};
SELECT 'map survives the round trip', getSetting('SQL_05210_map');
SELECT 'session is still usable', 1;
SELECT 'remote shard reads the setting', count() FROM remote('127.0.0.1', system.one) SETTINGS SQL_05210_map = {'k':'v'};

SET SQL_05210_comma_in_key = {'a,b':'v'};
SELECT 'comma inside a quoted key', getSetting('SQL_05210_comma_in_key');

SET SQL_05210_bracket_in_value = {'k':'v)'};
SELECT 'closing bracket inside a quoted value', getSetting('SQL_05210_bracket_in_value');

SET SQL_05210_two_keys = {'k':'v','k2':'v2'};
SELECT 'two keys', getSetting('SQL_05210_two_keys');

-- Control: an empty map restores on master too, and must keep restoring.
SET SQL_05210_empty = {};
SELECT 'empty map', getSetting('SQL_05210_empty');

-- Control: a scalar custom setting is unaffected.
SET SQL_05210_scalar = 'plain string';
SELECT 'scalar', getSetting('SQL_05210_scalar');
