SELECT isNullOrEmpty([]);
SELECT isNullOrEmpty([1, 2, 3]);
SELECT isNullOrEmpty(NULL);
SELECT isNullOrEmpty([NULL]);

SELECT isNullOrEmpty([[]]);
SELECT isNullOrEmpty([[[[]]]]);
SELECT isNullOrEmpty([[[[]], [[]]], []]);

SELECT isNullOrEmpty(2); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT isNullOrEmpty(tuple(1,2)); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT isNullOrEmpty('Hello'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT isNullOrEmpty(); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
