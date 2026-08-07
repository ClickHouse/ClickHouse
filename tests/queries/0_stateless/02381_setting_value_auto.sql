SELECT value, changed, type FROM system.settings WHERE name = 'insert_quorum';

SET insert_quorum = 'auto';
SELECT value, changed, type FROM system.settings WHERE name = 'insert_quorum';

SET insert_quorum = 0;
SELECT value, changed, type FROM system.settings WHERE name = 'insert_quorum';

SET insert_quorum = 1;
SELECT value, changed, type FROM system.settings WHERE name = 'insert_quorum';
