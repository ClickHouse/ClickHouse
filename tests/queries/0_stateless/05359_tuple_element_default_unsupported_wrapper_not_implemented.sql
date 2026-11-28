-- A DEFAULT inside a wrapper that does not support tuple-element defaults is rejected with
-- NOT_IMPLEMENTED, even if the default would also fail validation (self-reference, ambiguity).

CREATE TABLE t_nested_self (n Nested(x Tuple(a UInt8 DEFAULT n.x))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t_nested_ambiguous (n Nested(x Tuple(a UInt8, b UInt8 DEFAULT a))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t_array_self (c Array(Tuple(a UInt8 DEFAULT c))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t_map_ambiguous (c Map(String, Tuple(a UInt8, b UInt8 DEFAULT a))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t_saf_self (c SimpleAggregateFunction(anyLast, Tuple(a UInt8), Tuple(b UInt8 DEFAULT c))) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }

-- Self-reference in a supported position is still reported as such.
CREATE TABLE t_tuple_self (c Tuple(a UInt8 DEFAULT c.a)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
