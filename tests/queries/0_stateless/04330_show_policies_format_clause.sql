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

-- SHOW CREATE <entity> had the same greedy-name bug for every entity type when
-- no name is given: the trailing FORMAT / SETTINGS / INTO OUTFILE keyword was
-- consumed as the entity name. Cover all of them (plural form = no name).
EXPLAIN AST SHOW CREATE ROW POLICIES FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE MASKING POLICIES FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE POLICIES FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE USERS FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE ROLES FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE QUOTAS FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE PROFILES FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE USERS SETTINGS max_threads = 1;
EXPLAIN AST SHOW CREATE QUOTAS INTO OUTFILE 'test.tsv';

-- SHOW CREATE <entity> <name> FORMAT ... (a real name present) was already fine
-- and must stay so.
EXPLAIN AST SHOW CREATE USER u_04330 FORMAT TabSeparated;
EXPLAIN AST SHOW CREATE ROW POLICY p_04330 ON db.tbl FORMAT TabSeparated;

-- A name that is backtick-quoted is a real name even when it spells a keyword.
-- The formatter must quote such a name back, otherwise the formatted query is
-- re-parsed as the output clause and the AST formatting round-trip check (debug
-- builds) fails with LOGICAL_ERROR.
EXPLAIN AST SHOW ROW POLICIES `FORMAT`;
EXPLAIN AST SHOW ROW POLICIES `SETTINGS`;
EXPLAIN AST SHOW MASKING POLICIES `FORMAT`;
EXPLAIN AST SHOW CREATE USER `FORMAT`;
EXPLAIN AST SHOW CREATE QUOTA `SETTINGS`;
EXPLAIN AST SHOW CREATE ROW POLICY `FORMAT` ON db.tbl;

-- End to end: the statement now executes with a FORMAT clause. Filtering by a
-- unique short name keeps the output deterministic on a shared server.
DROP ROW POLICY IF EXISTS p_04330 ON db.tbl;
CREATE ROW POLICY p_04330 ON db.tbl USING 1 AS PERMISSIVE;
SHOW ROW POLICIES p_04330 FORMAT TabSeparated;
DROP ROW POLICY p_04330 ON db.tbl;
