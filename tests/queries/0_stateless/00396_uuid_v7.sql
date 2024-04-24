select '-- UUIDToNum --';
select UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 1) = UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 1);
select UUIDToNum(toUUID('00112233-4455-6677-8899-aabbccddeeff'), 2) = UUIDStringToNum('00112233-4455-6677-8899-aabbccddeeff', 2);

select '-- UUIDv7toDateTime --';
select UUIDv7ToDateTime(toUUID('018f05c9-4ab8-7b86-b64e-c9f03fbd45d1'), 'America/New_York');
