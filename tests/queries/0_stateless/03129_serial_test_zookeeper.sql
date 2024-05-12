-- Tags: zookeeper

SELECT serial('x');
SELECT serial('x');
SELECT serial('y');
SELECT serial('x') FROM numbers(5);

SELECT serial(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT serial('x', 'y'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT serial(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT serial('z'), serial('z') FROM numbers(5);
