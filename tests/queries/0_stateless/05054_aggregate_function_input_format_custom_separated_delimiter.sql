SET aggregate_function_input_format = 'value';

SELECT 'CustomSeparated with a tab delimiter accepts the escaped value forms';
SELECT finalizeAggregation(x), y FROM format(CustomSeparated, 'x AggregateFunction(any, UInt64), y UInt64', $$'42'	7
\"43\"	8
44	9
$$) SETTINGS format_custom_escaping_rule = 'Escaped', format_custom_field_delimiter = '\t';

SELECT 'CustomSeparated with a non-TSV delimiter after an AggregateFunction field is rejected';
SELECT finalizeAggregation(x), y FROM format(CustomSeparated, 'x AggregateFunction(any, UInt64), y UInt64', $$42;7
$$) SETTINGS format_custom_escaping_rule = 'Escaped', format_custom_field_delimiter = ';'; -- { serverError BAD_ARGUMENTS }

SELECT 'The state form is rejected too';
SELECT y FROM format(CustomSeparated, 'x AggregateFunction(any, UInt64), y UInt64', $$42;7
$$) SETTINGS format_custom_escaping_rule = 'Escaped', format_custom_field_delimiter = ';', aggregate_function_input_format = 'state'; -- { serverError BAD_ARGUMENTS }

