-- Tags: no-parallel
-- `ATTACH USER` creates a server-global user.

-- Stored `ATTACH USER` definitions use a numeric Unix timestamp. Dates after 2286 have more than
-- 10 digits and must follow the storage parsing path, not the query-time datetime parser.
ATTACH USER user_05023_valid_until_attach_numeric IDENTIFIED WITH no_password VALID UNTIL '253402250399';
SELECT toUnixTimestamp64Second(valid_until[1]) FROM system.users WHERE name = 'user_05023_valid_until_attach_numeric';
DROP USER user_05023_valid_until_attach_numeric;
