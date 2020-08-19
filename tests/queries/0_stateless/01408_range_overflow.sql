-- executeGeneric()
SELECT range(1025, 1048576 + 9223372036854775807, 9223372036854775807); -- { serverError 69; }
SELECT range(1025, 1048576 + (9223372036854775807 AS i), i); -- { serverError 69; }

-- executeConstStep()
SELECT range(number, 1048576 + 9223372036854775807, 9223372036854775807) FROM system.numbers LIMIT 1 OFFSET 1025; -- { serverError 69; }

-- executeConstStartStep()
SELECT range(1025, number + 9223372036854775807, 9223372036854775807) FROM system.numbers LIMIT 1 OFFSET 1048576; -- { serverError 69; }

-- executeConstStart()
SELECT range(1025, 1048576 + 9223372036854775807, number + 9223372036854775807) FROM system.numbers LIMIT 1; -- { serverError 69; }
