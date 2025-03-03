SET input_format_values_interpret_expressions = 0;
SET input_format_values_accurate_types_of_literals = 0;

CREATE TABLE IF NOT EXISTS 03246_range_literal_replacement_works (id UInt8) Engine=Memory;

INSERT INTO 03246_range_literal_replacement_works VALUES (1 BETWEEN 0 AND 2);

SELECT * FROM 03246_range_literal_replacement_works;

DROP TABLE IF EXISTS 03246_range_literal_replacement_works;
