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

SET allow_experimental_analyzer = 1;

--
-- Some examples with a tree
--
-- department structure represented here is as follows:
--
-- ROOT-+->A-+->B-+->C
--      |         |
--      |         +->D-+->F
--      +->E-+->G

DROP TABLE IF EXISTS department;
CREATE TABLE department (
    id UInt64,  -- department ID
    parent_department UInt64, -- upper department ID
    name String -- department name
)
ENGINE=TinyLog;

INSERT INTO department VALUES (0, NULL, 'ROOT');
INSERT INTO department VALUES (1, 0, 'A');
INSERT INTO department VALUES (2, 1, 'B');
INSERT INTO department VALUES (3, 2, 'C');
INSERT INTO department VALUES (4, 2, 'D');
INSERT INTO department VALUES (5, 0, 'E');
INSERT INTO department VALUES (6, 4, 'F');
INSERT INTO department VALUES (7, 5, 'G');


-- extract all departments under 'A'. Result should be A, B, C, D and F
WITH RECURSIVE subdepartment AS
(
    -- non recursive term
    SELECT name as root_name, * FROM department WHERE name = 'A'

    UNION ALL

    -- recursive term
    SELECT sd.root_name, d.* FROM department AS d, subdepartment AS sd
        WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

-- extract all departments under 'A' with "level" number
WITH RECURSIVE subdepartment AS
(
    -- non recursive term
    SELECT 1 AS level, * FROM department WHERE name = 'A'

    UNION ALL

    -- recursive term
    SELECT sd.level + 1, d.* FROM department AS d, subdepartment AS sd
        WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment ORDER BY name;

-- extract all departments under 'A' with "level" number.
-- Only shows level 2 or more
WITH RECURSIVE subdepartment AS
(
    -- non recursive term
    SELECT 1 AS level, * FROM department WHERE name = 'A'

    UNION ALL

    -- recursive term
    SELECT sd.level + 1, d.* FROM department AS d, subdepartment AS sd
        WHERE d.parent_department = sd.id
)
SELECT * FROM subdepartment WHERE level >= 2 ORDER BY name;

-- "RECURSIVE" is ignored if the query has no self-reference
WITH RECURSIVE subdepartment AS
(
    -- note lack of recursive UNION structure
    SELECT * FROM department WHERE name = 'A'
)
SELECT * FROM subdepartment ORDER BY name;

-- inside subqueries
SELECT count(*) FROM
(
    WITH RECURSIVE t AS (
        SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t WHERE n < 500
    )
    SELECT * FROM t
) AS t WHERE n < (
        SELECT count(*) FROM (
            WITH RECURSIVE t AS (
                   SELECT toUInt64(1) AS n UNION ALL SELECT n + 1 FROM t WHERE n < 100
                )
            SELECT * FROM t WHERE n < 50000
         ) AS t WHERE n < 100);

-- corner case in which sub-WITH gets initialized first
WITH RECURSIVE q AS (
      SELECT * FROM department
    UNION ALL
      (WITH x AS (SELECT * FROM q)
       SELECT * FROM x)
    )
SELECT * FROM q LIMIT 24;

WITH RECURSIVE q AS (
      SELECT * FROM department
    UNION ALL
      (WITH RECURSIVE x AS (
           SELECT * FROM department
         UNION ALL
           (SELECT * FROM q UNION ALL SELECT * FROM x)
        )
       SELECT * FROM x)
    )
SELECT * FROM q LIMIT 32;

-- recursive term has sub-UNION
WITH RECURSIVE t AS (
    SELECT 1 AS i, 2 AS j
    UNION ALL
    SELECT t2.i, t.j+1 FROM
        (SELECT 2 AS i UNION ALL SELECT 3 AS i) AS t2
        JOIN t ON (t2.i = t.i+1))

    SELECT * FROM t;

-- { echoOff }
