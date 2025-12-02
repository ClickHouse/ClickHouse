-- Tags: zookeeper

SELECT generateSerialID(currentDatabase() || 'x');
SELECT generateSerialID(currentDatabase() || 'x');
SELECT generateSerialID(currentDatabase() || 'y');
SELECT generateSerialID(currentDatabase() || 'x') FROM numbers(5);

SELECT generateSerialID(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateSerialID('x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateSerialID(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generateSerialID(materialize('x')); -- { serverError ILLEGAL_COLUMN }
SELECT generateSerialID('abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij'); -- { serverError BAD_ARGUMENTS }

-- Here the functions are identical and fall into common-subexpression-elimination:
SELECT generateSerialID(currentDatabase() || 'z'), generateSerialID(currentDatabase() || 'z') FROM numbers(5);

SET max_autoincrement_series = 3;
SELECT generateSerialID('a'); -- { serverError LIMIT_EXCEEDED }
