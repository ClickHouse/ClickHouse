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

WITH RECURSIVE foo AS
    (SELECT 1 AS i
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          UNION ALL
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

WITH RECURSIVE foo AS
    (SELECT 1 AS i
    UNION ALL
       SELECT * FROM
       (SELECT i+1 FROM foo WHERE i < 10
          UNION ALL
       SELECT i+1 FROM foo WHERE i < 5) AS t
) SELECT * FROM foo;

WITH RECURSIVE foo AS
    (SELECT 1 AS i
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          EXCEPT
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

WITH RECURSIVE foo AS
    (SELECT 1 AS i
    UNION ALL
       (SELECT i+1 FROM foo WHERE i < 10
          INTERSECT
       SELECT i+1 FROM foo WHERE i < 5)
) SELECT * FROM foo;

--
-- test for nested-recursive-WITH bug
--
WITH RECURSIVE t AS (
    WITH RECURSIVE s AS (
        SELECT toUInt64(1) AS i
        UNION ALL
        SELECT i+1 FROM s WHERE i < 10
    )
    SELECT i AS j FROM s
    UNION ALL
    SELECT j+1 FROM t WHERE j < 10
)
SELECT * FROM t;

--
-- Test CTEs read in non-initialization orders
--

WITH RECURSIVE
  tab AS (SELECT * FROM values('id_key UInt64, link UInt64', (1,17), (2,17), (3,17), (4,17), (6,17), (5,17))),
  iter AS (
      SELECT 0 AS id_key, 'base' AS row_type, 17 AS link
    UNION ALL (
      WITH remaining AS (
        SELECT tab.id_key AS id_key, 'true'::text AS row_type, iter.link AS link, MIN(tab.id_key) OVER () AS min
        FROM tab INNER JOIN iter USING (link)
        WHERE tab.id_key > iter.id_key
      ),
      first_remaining AS (
        SELECT id_key, row_type, link
        FROM remaining
        WHERE id_key=min
      ),
      effect AS (
        SELECT tab.id_key AS id_key, 'new'::text AS row_type, tab.link AS link
        FROM first_remaining e INNER JOIN tab ON e.id_key=tab.id_key
        WHERE e.row_type = 'false'
      )
      SELECT * FROM first_remaining
      UNION ALL SELECT * FROM effect
    )
  )
SELECT * FROM iter;

-- { echoOff }
