-- https://github.com/ClickHouse/ClickHouse/issues/117201
-- `bech32Encode` returns an empty string on any encoding error - most easily, data too long for a
-- valid Bech32 string - so arbitrarily many distinct inputs share that result. It must not claim to
-- be injective, or the default-on `optimize_injective_functions_in_group_by` and
-- `optimize_injective_functions_in_limit_by` group and limit by the raw argument instead.

SELECT 'ground truth';
SELECT DISTINCT bech32Encode('bc', repeat('x', 60) || toString(number)) = '' FROM numbers(4);

SELECT 'group by';
SELECT count() FROM (SELECT 1 FROM numbers(4) GROUP BY bech32Encode('bc', repeat('x', 60) || toString(number)));
SELECT count() FROM (SELECT 1 FROM numbers(4) GROUP BY bech32Encode('bc', repeat('x', 60) || toString(number))) SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT 'limit by';
SELECT count() FROM (SELECT bech32Encode('bc', repeat('x', 60) || toString(number)) AS k FROM numbers(4) LIMIT 1 BY k);
SELECT count() FROM (SELECT bech32Encode('bc', repeat('x', 60) || toString(number)) AS k FROM numbers(4) LIMIT 1 BY k) SETTINGS optimize_injective_functions_in_limit_by = 0;

SELECT 'valid data still round-trips';
SELECT bech32Decode(bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0)).2 = unhex('751e76e8199196d454941c45d1b3a323f1433bd6');
