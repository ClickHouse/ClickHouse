SELECT UUIDNumToString(toFixedString(unhex('0123456789ABCDEF0123456789ABCDEF' AS hex) AS bytes, 16) AS uuid_binary) AS uuid_string, hex(UUIDStringToNum(uuid_string)) = hex AS test1, UUIDStringToNum(uuid_string) = bytes AS test2;
SELECT UUIDNumToString(toFixedString(unhex(materialize('0123456789ABCDEF0123456789ABCDEF') AS hex) AS bytes, 16) AS uuid_binary) AS uuid_string, hex(UUIDStringToNum(uuid_string)) = hex AS test1, UUIDStringToNum(uuid_string) = bytes AS test2;
SELECT hex(UUIDStringToNum('01234567-89ab-cdef-0123-456789abcdef'));
SELECT hex(UUIDStringToNum(materialize('01234567-89ab-cdef-0123-456789abcdef')));
SELECT '01234567-89ab-cdef-0123-456789abcdef' AS str, UUIDNumToString(UUIDStringToNum(str)), UUIDNumToString(UUIDStringToNum(toFixedString(str, 36)));
SELECT materialize('01234567-89ab-cdef-0123-456789abcdef') AS str, UUIDNumToString(UUIDStringToNum(str)), UUIDNumToString(UUIDStringToNum(toFixedString(str, 36)));
SELECT toString(toUUID('3f1ed72e-f7fe-4459-9cbe-95fe9298f845'));
