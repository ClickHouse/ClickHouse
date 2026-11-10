-- Tuple-related queries from tests/queries/0_stateless/02940_variant_text_deserialization.sql.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET session_timezone = 'UTC';

SELECT 'Tuple';
SELECT v, variantElement(v, 'Tuple(a UInt64, b UInt64)') FROM format(JSONEachRow, 'v Variant(String, Tuple(a UInt64, b UInt64))', '{"v" : null}, {"v" : "string"}, {"v" : {"a" : 42, "b" : null}}, {"v" : {"a" : 44, "d" : 32}}') FORMAT JSONEachRow;
SELECT v, variantElement(v, 'Tuple(a UInt64, b UInt64)') FROM format(JSONEachRow, 'v Variant(String, Tuple(a UInt64, b UInt64))', '{"v" : null}, {"v" : "string"}, {"v" : {"a" : 42, "b" : null}}, {"v" : {"a" : 44, "d" : 32}}') SETTINGS input_format_json_defaults_for_missing_elements_in_named_tuple=0;
