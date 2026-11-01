-- Test SQL standard OVERLAY syntax: OVERLAY(string PLACING replacement FROM start [FOR length])

-- Basic 4-argument form with FOR
SELECT 'Basic OVERLAY with FOR';
SELECT OVERLAY('Hello World' PLACING 'SQL' FROM 7 FOR 5);
SELECT OVERLAY('abcdef' PLACING 'XY' FROM 3 FOR 2);

-- 3-argument form without FOR
SELECT 'OVERLAY without FOR';
SELECT OVERLAY('abcdef' PLACING 'XY' FROM 3);
SELECT OVERLAY('Spark SQL' PLACING '_' FROM 6);

-- Empty replacement (deletion)
SELECT 'OVERLAY with empty replacement';
SELECT OVERLAY('Hello World' PLACING '' FROM 6 FOR 6);

-- Equivalence with functional syntax
SELECT 'Equivalence: SQL standard vs functional';
SELECT OVERLAY('Spark SQL' PLACING 'ANSI ' FROM 7 FOR 0) = overlay('Spark SQL', 'ANSI ', 7, 0);
SELECT OVERLAY('Spark SQL' PLACING '_' FROM 6) = overlay('Spark SQL', '_', 6);

-- overlayUTF8 with SQL standard syntax
SELECT 'overlayUTF8 with SQL standard syntax';
SELECT overlayUTF8('Spark SQL和CH' PLACING '_' FROM 6);
SELECT overlayUTF8('Spark SQL和CH' PLACING 'ANSI ' FROM 7 FOR 0);

-- Case insensitivity of OVERLAY keyword
SELECT 'Case insensitivity';
SELECT overlay('abcdef' PLACING 'XY' FROM 3 FOR 2);

-- Functional syntax still works
SELECT 'Functional syntax still works';
SELECT overlay('abcdef', 'XY', 3, 2);
SELECT overlay('abcdef', 'XY', 3);

-- Expressions as arguments
SELECT 'Expressions as arguments';
SELECT OVERLAY('Hello World' PLACING concat('S', 'QL') FROM 5 + 2 FOR 10 - 5);

-- Mixed keyword/comma syntax must be rejected
SELECT OVERLAY('abcdef' PLACING 'XY', 3); -- { clientError SYNTAX_ERROR }
SELECT OVERLAY('abcdef' PLACING 'XY', 3, 2); -- { clientError SYNTAX_ERROR }
SELECT overlay('abcdef', 'XY' FROM 3); -- { clientError SYNTAX_ERROR }

-- Keyword mode requires FROM before closing bracket
SELECT OVERLAY('abcdef' PLACING 'XY'); -- { clientError SYNTAX_ERROR }
