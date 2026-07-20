SELECT *
FROM format(
    Values,
    'u UInt8, dt DateTime(\'UTC\'), dt64 DateTime64(3, \'UTC\'), t Time, t64 Time64(3), id UUID',
    $$(256, 18446744073709551616, 18446744073709551616, 18446744073709551616, 18446744073709551616,
       '\x3550e8400-e29b-41d4-a716-446655440000')$$)
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0;

SELECT * FROM format(Values, 'x Array(UInt8)', '([256])')
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0;

SELECT * FROM format(Values, 'x Array(UInt8)', $$('[256]')$$)
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT CAST(x AS Array(Int8)) FROM format(Values, 'x QBit(Int8, 1)', '([256])')
SETTINGS allow_experimental_qbit_type = 1, input_format_values_interpret_expressions = 0,
    input_format_values_deduce_templates_of_expressions = 0;

SELECT x, dynamicType(x) FROM format(Values, 'x Dynamic', '([+12])')
SETTINGS allow_experimental_dynamic_type = 1;

SELECT x, variantType(x) FROM format(Values, 'x Variant(UInt8, UInt16)', '(256)')
SETTINGS allow_suspicious_variant_types = 1, input_format_values_interpret_expressions = 0,
    input_format_values_deduce_templates_of_expressions = 0;

SELECT x, variantType(x)
FROM format(Values, 'x Variant(UUID, String)', $$('\x3550e8400-e29b-41d4-a716-446655440000')$$)
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0;

SELECT CAST(x AS Array(Int8)), variantType(x)
FROM format(Values, 'x Variant(QBit(Int8, 1), String)', '([256])')
SETTINGS allow_experimental_qbit_type = 1, input_format_values_interpret_expressions = 0,
    input_format_values_deduce_templates_of_expressions = 0;

SELECT *
FROM format(Values, 'x UUID', $$('\x3550e8400-e29b-41d4-a716-446655440000suffix')$$)
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0;

SELECT * FROM format(Values, 'x UInt8', '(012)')
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT * FROM format(Values, 'x UInt8', '(+12)')
SETTINGS input_format_values_interpret_expressions = 0, input_format_values_deduce_templates_of_expressions = 0; -- { serverError SUPPORT_IS_DISABLED }
