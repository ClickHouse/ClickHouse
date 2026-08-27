SET enable_analyzer = 0;

-- EXPLAIN SYNTAX with the old analyzer calls parseArguments, which rejects empty script_name
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String'); -- { serverError BAD_ARGUMENTS }
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', SETTINGS max_command_execution_time=100); -- { serverError BAD_ARGUMENTS }
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', SETTINGS max_command_execution_time=100, command_read_timeout=1); -- { serverError BAD_ARGUMENTS }


SET enable_analyzer = 1;

-- EXPLAIN SYNTAX with the new analyzer does not call parseArguments
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String');
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', SETTINGS max_command_execution_time=100);
SELECT '--------------------';
EXPLAIN SYNTAX SELECT * from executable('', 'JSON', 'data String', SETTINGS max_command_execution_time=100, command_read_timeout=1);
SELECT '--------------------';

SELECT * from executable('JSON', 'data String', SETTINGS max_command_execution_time=100); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * from executable('JSON', 'data String', 'TEST', 'TEST'); -- { serverError BAD_ARGUMENTS }
