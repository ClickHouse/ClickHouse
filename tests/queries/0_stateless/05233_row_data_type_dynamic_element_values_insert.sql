SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_dynamic_values;

CREATE TABLE row_dynamic_values
(
    id UInt8,
    r Row(x Dynamic, y UInt8)
)
ENGINE = MergeTree ORDER BY id;

-- Without template deduction, the expressions below go through the expression fallback of the VALUES
-- parser. A Dynamic element must keep the type of the expression there instead of the type a bare
-- Field carries. The second insert covers the template path.
SET input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO row_dynamic_values VALUES (1, (1::UInt32, 0)), (2, ('a'::FixedString(1), 1)), (3, (toDate('2020-01-01'), 2));

SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO row_dynamic_values VALUES (4, (1::UInt32, 0)), (5, ('a'::FixedString(1), 1)), (6, (toDate('2020-01-01'), 2));

SELECT id, __rowElement(r, 'x'), dynamicType(__rowElement(r, 'x')), __rowElement(r, 'y') FROM row_dynamic_values ORDER BY id;

DROP TABLE row_dynamic_values;
