-- The interpreted VALUES path must keep descending into nested named tuples when an outer tuple level
-- is converted positionally (a side is unnamed or the element names are disjoint), like INSERT ... SELECT.

SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;
SET input_format_values_interpret_expressions = 1;
SET input_format_values_deduce_templates_of_expressions = 0;

DROP TABLE IF EXISTS t_values_positional_outer;
CREATE TABLE t_values_positional_outer (x Tuple(y Tuple(a Int32, b Int32))) ENGINE = Memory;

-- Disjoint outer names: the outer level is positional, the inner tuple is matched by name.
INSERT INTO t_values_positional_outer VALUES (tuple('x')(tuple('b', 'a')(1, 2)));
INSERT INTO t_values_positional_outer SELECT tuple('x')(tuple('b', 'a')(1, 2));
-- Unnamed outer tuple: same.
INSERT INTO t_values_positional_outer VALUES (tuple(tuple('b', 'a')(3, 4)));
INSERT INTO t_values_positional_outer SELECT tuple(tuple('b', 'a')(3, 4));
SELECT x.y.a, x.y.b FROM t_values_positional_outer ORDER BY x.y.a;

-- The inner conversion would lose the source field `b2`: rejected on both paths.
INSERT INTO t_values_positional_outer VALUES (tuple('x')(tuple('a', 'b2')(5, 6))); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_positional_outer SELECT tuple('x')(tuple('a', 'b2')(5, 6)); -- { serverError CANNOT_CONVERT_TYPE }
INSERT INTO t_values_positional_outer VALUES (tuple(tuple('a', 'b2')(5, 6))); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_positional_outer SELECT tuple(tuple('a', 'b2')(5, 6)); -- { serverError CANNOT_CONVERT_TYPE }

SELECT count() FROM t_values_positional_outer;
DROP TABLE t_values_positional_outer;
