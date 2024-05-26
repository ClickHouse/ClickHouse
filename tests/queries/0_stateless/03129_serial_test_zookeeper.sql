-- Tags: zookeeper

SELECT generateSerialID('x');
SELECT generateSerialID('x');
SELECT generateSerialID('y');
SELECT generateSerialID('x') FROM numbers(5);

SELECT generateSerialID(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateSerialID('x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT generateSerialID(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT generateSerialID('z'), generateSerialID('z') FROM numbers(5);
