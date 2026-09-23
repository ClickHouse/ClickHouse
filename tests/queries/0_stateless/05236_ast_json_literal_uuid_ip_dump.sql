-- `FieldVisitorDump` writes UUID / IPv4 / IPv6 values as `UUID_'...'` etc., but `Field::restoreFromDump` could not read
-- them back, so a `Literal` of these types could not be deserialized from the JSON AST. Found by json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UUID","value":"UUID_''61f0c404-5cb3-11e7-907b-a6006ad3dba0''"}}');
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"IPv4","value":"IPv4_''1.2.3.4''"}}');
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"IPv6","value":"IPv6_''2001:db8::1''"}}');
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"Int128","value":"Int128_-170141183460469231731687303715884105728"}}');

-- A dump payload of another type, or garbage, is rejected as BAD_ARGUMENTS (it used to leak CANNOT_PARSE_QUOTED_STRING).
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UUID","value":"''VALUE&"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UUID","value":"Int128_1"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UUID","value":"UUID"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UUID","value":"UUID_''not-a-uuid''"}}'); -- { serverError CANNOT_PARSE_UUID }

-- The whole dump payload has to be consumed: trailing bytes after the quoted value are an error, not silently dropped.
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"UUID","value":"UUID_''61f0c404-5cb3-11e7-907b-a6006ad3dba0''junk"}}'); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"IPv4","value":"IPv4_''1.2.3.4''x"}}'); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"IPv6","value":"IPv6_''::1''x"}}'); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
