-- The MongoDB shell `ISODate("...")` wrapper is understood only where the target type is already
-- known to be `DateTime64`. It is deliberately not part of JSON schema inference, so an inferred
-- schema, `Dynamic` and `JSON` keep rejecting it.

SELECT ts FROM format(JSONEachRow, 'ts DateTime64(3, \'UTC\')', '{"ts": ISODate("2024-05-29T23:16:12.256Z")}');
SELECT ts FROM format(JSONEachRow, 'ts Nullable(DateTime64(3, \'UTC\'))', '{"ts": new ISODate("2024-05-29T23:16:12.256Z")}');

SELECT ts FROM format(JSONEachRow, '{"ts": ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT ts FROM format(JSONEachRow, 'ts Dynamic', '{"ts": ISODate("2024-05-29T23:16:12.256Z")}'); -- { serverError INCORRECT_DATA }
SELECT j FROM format(JSONEachRow, 'j JSON', '{"j": {"ts": ISODate("2024-05-29T23:16:12.256Z")}}'); -- { serverError INCORRECT_DATA }
