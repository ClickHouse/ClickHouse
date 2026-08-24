-- These queries yield syntax error, not logical error.

CREATE TEMPORARY TABLE test (ad DEFAULT *); -- { clientError SYNTAX_ERROR }
CREATE TEMPORARY TABLE test (ad INT DEFAULT *); -- { clientError SYNTAX_ERROR }
CREATE TEMPORARY TABLE test (ad DEFAULT * NOT NULL); -- { clientError SYNTAX_ERROR }
CREATE TEMPORARY TABLE test (ad DEFAULT t.* NOT NULL); -- { clientError SYNTAX_ERROR }
