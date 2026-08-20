-- Numeric 1/0 in JSON Bool parsing must honor allow_special_bool_values, mirroring the
-- text/CSV paths. Bool has higher Variant priority than the integer types, so without the
-- gate unquoted numeric 1/0 would greedily land in Bool during Variant/Dynamic JSON
-- inference. By default it must fall through to the wider integer; with the opt-in it
-- resolves to Bool.
SET enable_variant_type = 1;

SET allow_special_bool_values_inside_variant = 0;
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Bool, UInt32)', '{"v":1}');
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Bool, UInt32)', '{"v":0}');

SET allow_special_bool_values_inside_variant = 1;
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Bool, UInt32)', '{"v":1}');
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Bool, UInt32)', '{"v":0}');

-- A plain Bool JSON column keeps accepting numeric 1/0 (base allow_special_bool_values
-- defaults to true and is only overridden internally by Variant inference).
SELECT * FROM format(JSONEachRow, 'x Bool', '{"x":1}');
SELECT * FROM format(JSONEachRow, 'x Bool', '{"x":0}');
SELECT * FROM format(JSONEachRow, 'x Bool', '{"x":true}');
SELECT * FROM format(JSONEachRow, 'x Bool', '{"x":false}');
