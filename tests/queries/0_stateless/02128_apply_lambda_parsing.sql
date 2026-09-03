WITH * APPLY lambda(e); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(1); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(x); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(range(1)); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(range(x)); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(1, 2); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(x, y); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda((x, y), 2); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda((x, y), x + y); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(tuple(1), 1); -- { clientError SYNTAX_ERROR }
SELECT * APPLY lambda(tuple(x), 1) FROM numbers(5);
SELECT * APPLY lambda(tuple(x), x + 1) FROM numbers(5);
