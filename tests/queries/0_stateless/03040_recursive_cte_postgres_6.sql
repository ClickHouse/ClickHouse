/**
  * Based on https://github.com/postgres/postgres/blob/master/src/test/regress/sql/with.sql, license:
  *
  * PostgreSQL Database Management System
  * (formerly known as Postgres, then as Postgres95)
  *
  * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
  *
  * Portions Copyright (c) 1994, The Regents of the University of California
  *
  * Permission to use, copy, modify, and distribute this software and its
  * documentation for any purpose, without fee, and without a written agreement
  * is hereby granted, provided that the above copyright notice and this
  * paragraph and the following two paragraphs appear in all copies.
  *
  * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
  * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
  * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
  * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
  * POSSIBILITY OF SUCH DAMAGE.
  *
  * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
  * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
  * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
  * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
  *PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
  */

--
-- Tests for common table expressions (WITH query, ... SELECT ...)
--

-- { echoOn }

SET enable_analyzer = 1;

--
-- error cases
--

-- INTERSECT
WITH RECURSIVE x AS (SELECT 1 AS n INTERSECT SELECT n+1 FROM x)
    SELECT * FROM x; -- {serverError UNSUPPORTED_METHOD}

WITH RECURSIVE x AS (SELECT 1 AS n INTERSECT ALL SELECT n+1 FROM x)
    SELECT * FROM x; -- {serverError UNSUPPORTED_METHOD}

-- EXCEPT
WITH RECURSIVE x AS (SELECT 1 AS n EXCEPT SELECT n+1 FROM x)
    SELECT * FROM x; -- {serverError UNSUPPORTED_METHOD}

WITH RECURSIVE x AS (SELECT 1 AS n EXCEPT ALL SELECT n+1 FROM x)
    SELECT * FROM x; -- {serverError UNSUPPORTED_METHOD}

-- no non-recursive term
WITH RECURSIVE x AS (SELECT n FROM x)
    SELECT * FROM x; -- {serverError UNKNOWN_TABLE}

-- recursive term in the left hand side (strictly speaking, should allow this)
WITH RECURSIVE x AS (SELECT n FROM x UNION ALL SELECT 1 AS n)
    SELECT * FROM x; -- {serverError UNKNOWN_TABLE}

DROP TABLE IF EXISTS y;
CREATE TABLE y (a UInt64) ENGINE=TinyLog;
INSERT INTO y SELECT * FROM numbers(1, 10);

-- LEFT JOIN

WITH RECURSIVE x AS (SELECT a AS n FROM y WHERE a = 1
    UNION ALL
    SELECT x.n+1 FROM y LEFT JOIN x ON x.n = y.a WHERE n < 10)
SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- RIGHT JOIN
WITH RECURSIVE x AS (SELECT a AS n FROM y WHERE a = 1
    UNION ALL
    SELECT x.n+1 FROM x RIGHT JOIN y ON x.n = y.a WHERE n < 10)
SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- FULL JOIN
WITH RECURSIVE x AS (SELECT a AS n FROM y WHERE a = 1
    UNION ALL
    SELECT x.n+1 FROM x FULL JOIN y ON x.n = y.a WHERE n < 10)
SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- subquery
WITH RECURSIVE x AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM x
                          WHERE n IN (SELECT * FROM x))
  SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- aggregate functions
WITH RECURSIVE x AS (SELECT 1 AS n UNION ALL SELECT count(*) FROM x)
  SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

WITH RECURSIVE x AS (SELECT 1 AS n UNION ALL SELECT sum(n) FROM x)
  SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- ORDER BY
WITH RECURSIVE x AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM x ORDER BY 1)
  SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- target list has a recursive query name
WITH RECURSIVE x AS (SELECT 1 AS id
    UNION ALL
    SELECT (SELECT * FROM x) FROM x WHERE id < 5
) SELECT * FROM x; -- { serverError UNKNOWN_TABLE }

-- mutual recursive query (not implemented)
WITH RECURSIVE
  x AS (SELECT 1 AS id UNION ALL SELECT id+1 FROM y WHERE id < 5),
  y AS (SELECT 1 AS id UNION ALL SELECT id+1 FROM x WHERE id < 5)
SELECT * FROM x FORMAT NULL SETTINGS max_recursive_cte_evaluation_depth = 5; -- { serverError TOO_DEEP_RECURSION }

-- { echoOff }
