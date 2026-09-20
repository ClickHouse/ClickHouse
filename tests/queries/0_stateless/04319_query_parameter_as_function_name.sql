-- A query parameter cannot be a function name or a part of a function name.
SELECT {f:Identifier}(x); -- { clientError SYNTAX_ERROR }
SELECT {f:Identifier}.g(x); -- { clientError SYNTAX_ERROR }

-- The original fuzzer reproducer.
DESCRIBE TABLE (SELECT {db:Identifier}.test_table.COLUMNS(plus(id, toInt256(x))) FROM test_table); -- { clientError SYNTAX_ERROR }
