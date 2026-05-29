-- LOCALTIMESTAMP and LOCALTIME are SQL-standard / PostgreSQL aliases for now().

-- Result type matches now().
SELECT toTypeName(LOCALTIMESTAMP) = toTypeName(now());
SELECT toTypeName(LOCALTIME) = toTypeName(now());

-- The value matches now() (both are constant-folded to the same instant within a query).
SELECT LOCALTIMESTAMP = now();
SELECT LOCALTIME = now();

-- The aliases are case-insensitive, like current_timestamp.
SELECT localtimestamp = now();
SELECT LocalTimeStamp = now();
SELECT localtime = now();
SELECT LocalTime = now();
