-- IN with non-constant tuple second argument must not CAST tuple
-- elements down to the LHS type, which would silently overflow (issue #103055)
-- Example: `toUInt8(232) IN (1000, number)` used to return 1 because CAST(1000, 'UInt8') = 232

SELECT toUInt8(232) IN (1000, 0);
SELECT toUInt8(232) IN (1000, number) FROM numbers(1);
SELECT toUInt8(0) IN (256, 999);
SELECT toUInt8(0) IN (256, number + 999) FROM numbers(1);

SELECT toUInt8(232) NOT IN (1000, 0);
SELECT toUInt8(232) NOT IN (1000, number) FROM numbers(1);

SELECT toUInt8(232) IN [1000, number] FROM numbers(1);

SELECT toUInt8(5) IN (1000, number + 5) FROM numbers(1);
