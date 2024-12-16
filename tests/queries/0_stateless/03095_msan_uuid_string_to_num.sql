SELECT UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 2);
SELECT UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', materialize(2)); -- { serverError ILLEGAL_COLUMN }
SELECT 'a/<@];!~p{jTj={)' AS bytes, UUIDNumToString(toFixedString(bytes, 16), 2) AS uuid;
SELECT 'a/<@];!~p{jTj={)' AS bytes, UUIDNumToString(toFixedString(bytes, 16), materialize(2)) AS uuid; -- { serverError ILLEGAL_COLUMN }
