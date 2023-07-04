CREATE TABLE test_table (value String) ENGINE=ExecutablePool('nonexist.py', 'TabSeparated', (foobar)); -- {serverError BAD_ARGUMENTS}
CREATE TABLE test_table (value String) ENGINE=ExecutablePool('nonexist.py', 'TabSeparated', '(SELECT 1)'); -- {serverError BAD_ARGUMENTS}
CREATE TABLE test_table (value String) ENGINE=ExecutablePool('nonexist.py', 'TabSeparated', [1,2,3]); -- {serverError BAD_ARGUMENTS}

