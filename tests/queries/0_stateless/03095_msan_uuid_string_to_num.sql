SELECT UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', materialize(2)) -- { serverError ILLEGAL_COLUMN }
