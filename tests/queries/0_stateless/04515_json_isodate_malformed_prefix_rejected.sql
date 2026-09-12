-- checkString() consumes "ISODate"/"new ISODate" even on a partial match, so a near-miss like
-- "ISODate123" must not fall through and get silently accepted as the integer 123.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate123}'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": new ISODate123}'); -- { serverError CANNOT_PARSE_DATETIME }
