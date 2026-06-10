-- Tags: no-parallel

-- SHOW [ROW|MASKING] POLICIES used to reject a trailing FORMAT / SETTINGS /
-- INTO OUTFILE clause: the optional bare-identifier short name greedily
-- consumed the keyword (e.g. FORMAT) instead of leaving it for the outer
-- ParserQueryWithOutput. EXPLAIN AST checks the parser only, so it works
-- without the enterprise-only system.masking_policies table and regardless of
-- which policies already exist on the server.

EXPLAIN AST SHOW ROW POLICIES FORMAT TabSeparated;
EXPLAIN AST SHOW POLICIES FORMAT TabSeparated;
EXPLAIN AST SHOW MASKING POLICIES FORMAT TabSeparated;
EXPLAIN AST SHOW ROW POLICIES SETTINGS max_threads = 1;
EXPLAIN AST SHOW ROW POLICIES INTO OUTFILE 'test.tsv';

-- A real bare-identifier short name is still accepted, and a FORMAT clause
-- after it is parsed as the output format, not folded into the name.
EXPLAIN AST SHOW ROW POLICIES p_04330 FORMAT TabSeparated;
EXPLAIN AST SHOW ROW POLICIES ON db.tbl FORMAT TabSeparated;

-- End to end: the statement now executes with a FORMAT clause. Filtering by a
-- unique short name keeps the output deterministic on a shared server.
DROP ROW POLICY IF EXISTS p_04330 ON db.tbl;
CREATE ROW POLICY p_04330 ON db.tbl USING 1 AS PERMISSIVE;
SHOW ROW POLICIES p_04330 FORMAT TabSeparated;
DROP ROW POLICY p_04330 ON db.tbl;
