SELECT OCTET_LENGTH('1234');
SELECT OcTet_lenGtH('1234');
SELECT OCTET_LENGTH('你好，世界');

-- This is a implementation-specific behavior of getting the length of an array.
SELECT OCTET_LENGTH([1,2,3,4]);
