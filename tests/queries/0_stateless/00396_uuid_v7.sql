SELECT '-- UUIDToNum --';
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 1) = UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 1);
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 2) = UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 2);
SELECT UUIDToNum();  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 3); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT UUIDToNum('00112233-4455-6677-8899-aabbccddeeff', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), '1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), materialize(1)); -- { serverError ILLEGAL_COLUMN }

SELECT '-- UUIDv7toDateTime --';
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York');
SELECT UUIDv7ToDateTime(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 1); -- { serverError ILLEGAL_COLUMN }
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York', 1);  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT UUIDv7ToDateTime('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1');  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/NewYork'); -- { serverError BAD_ARGUMENTS }
SELECT UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), materialize('America/New_York')); -- { serverError ILLEGAL_COLUMN }

