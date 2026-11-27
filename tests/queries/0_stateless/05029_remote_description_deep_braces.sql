-- Nested braces in the address of a table function are expanded recursively, and the limit on the
-- number of generated addresses does not bound the nesting depth.

SELECT * FROM url(concat(repeat('{', 100000), ',', repeat('}', 100000))); -- { serverError TOO_DEEP_RECURSION }
