-- Test: exercises `JSONColumnsBlockInputFormatBase::read` force-null check
-- Covers: src/Processors/Formats/Impl/JSONColumnsBlockInputFormatBase.cpp:206 — the
--   `format_settings.force_null_for_omitted_fields && !isNullableOrLowCardinalityNullable(fields[i].type)`
--   branch for the JSONColumns / JSONCompactColumns / JSONColumnsWithMetadata formats.
-- The PR adds this check but the PR's own test only covers JSONEachRow / BSONEachRow / TSKV /
-- (T|C)SVWithNamesAndTypes — the JSONColumns* family is completely untested.

set allow_suspicious_low_cardinality_types = 1;

-- JSONColumns (name-keyed): default behaviour fills omitted with type default.
select * from (select * from format(JSONColumns, 'foo UInt32, bar UInt32', '{"foo":[1,2,3]}')) order by foo;

-- Setting on, omitted column is non-Nullable -> must throw TYPE_MISMATCH.
select * from format(JSONColumns, 'foo UInt32, bar UInt32', '{"foo":[1,2,3]}') settings input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }

-- Setting on, omitted column is Nullable -> must succeed and fill with NULL.
select * from (select * from format(JSONColumns, 'foo UInt32, bar Nullable(UInt32)', '{"foo":[1,2,3]}') settings input_format_force_null_for_omitted_fields = 1) order by foo;

-- Setting on, omitted column is LowCardinality(Nullable(...)) -> must succeed and fill with NULL.
select * from (select * from format(JSONColumns, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', '{"foo":[1,2,3]}') settings input_format_force_null_for_omitted_fields = 1) order by foo;

-- JSONCompactColumns (positional): omitted trailing column also flows through the same code path.
select * from format(JSONCompactColumns, 'foo UInt32, bar UInt32', '[[1,2,3]]') settings input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }
select * from (select * from format(JSONCompactColumns, 'foo UInt32, bar Nullable(UInt32)', '[[1,2,3]]') settings input_format_force_null_for_omitted_fields = 1) order by foo;

-- JSONColumnsWithMetadata: same shared base class.
select * from format(JSONColumnsWithMetadata, 'foo UInt32, bar UInt32', '{"meta": [{"name":"foo","type":"UInt32"}], "data": {"foo":[1,2,3]}}') settings input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }
select * from (select * from format(JSONColumnsWithMetadata, 'foo UInt32, bar Nullable(UInt32)', '{"meta": [{"name":"foo","type":"UInt32"}], "data": {"foo":[1,2,3]}}') settings input_format_force_null_for_omitted_fields = 1) order by foo;
