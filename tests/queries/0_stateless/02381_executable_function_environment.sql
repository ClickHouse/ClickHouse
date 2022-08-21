EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', ENVIRONMENT VAR1 = 'a');
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', ENVIRONMENT VAR1 = 'a', VAR2 = 1);
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', ENVIRONMENT VAR1 = 'a', SETTINGS max_command_execution_time=100, command_read_timeout=1);
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', ENVIRONMENT VAR1 = 'a', VAR2 = 1, SETTINGS max_command_execution_time=100, command_read_timeout=1);
