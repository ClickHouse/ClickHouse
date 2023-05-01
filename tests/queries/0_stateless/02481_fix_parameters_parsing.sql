SELECT func(1)(2)(3); -- { clientError SYNTAX_ERROR }
SELECT * FROM VALUES(1)(2); -- { clientError SYNTAX_ERROR }
