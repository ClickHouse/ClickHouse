-- Malformed `ISODate` wrappers are rejected. In particular a near-miss like `ISODate123` must not be
-- accepted as the number `123`: a number never starts with `I` or `n`, so these tokens can only be a wrapper.

SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate123}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": new ISODate123}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate(1716938172)}'); -- { serverError CANNOT_PARSE_QUOTED_STRING }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate("2024-05-29T23:16:12.256Z"}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate("not a date")}'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT * FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', '{"ts": ISODate123}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONEachRow, 'ts Nullable(DateTime64(3))', '{"ts": new Foo(1)}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', '{"v": ISODate123}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
