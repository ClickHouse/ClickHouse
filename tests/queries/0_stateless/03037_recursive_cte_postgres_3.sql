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
-- different tree example
--
DROP TABLE IF EXISTS tree;
CREATE TABLE tree(
    id UInt64,
    parent_id Nullable(UInt64)
)
ENGINE=TinyLog;

INSERT INTO tree
VALUES (1, NULL), (2, 1), (3,1), (4,2), (5,2), (6,2), (7,3), (8,3), (9,4), (10,4), (11,7), (12,7), (13,7), (14, 9), (15,11), (16,11);

--
-- get all paths from "second level" nodes to leaf nodes
--
WITH RECURSIVE t AS (
    SELECT 1 AS id, []::Array(UInt64) AS path
UNION ALL
    SELECT tree.id, arrayConcat(t.path, [tree.id])
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.*, t2.* FROM t AS t1 JOIN t AS t2 ON
    (t1.path[1] = t2.path[1] AND
    length(t1.path) = 1 AND
    length(t2.path) > 1)
    ORDER BY t1.id, t2.id;

-- just count 'em
WITH RECURSIVE t AS (
    SELECT 1 AS id, []::Array(UInt64) AS path
UNION ALL
    SELECT tree.id, arrayConcat(t.path, [tree.id])
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.id, count(t2.path) FROM t AS t1 JOIN t AS t2 ON
    (t1.path[1] = t2.path[1] AND
    length(t1.path) = 1 AND
    length(t2.path) > 1)
    GROUP BY t1.id
    ORDER BY t1.id;

-- -- this variant tickled a whole-row-variable bug in 8.4devel
WITH RECURSIVE t AS (
    SELECT 1 AS id, []::Array(UInt64) AS path
UNION ALL
    SELECT tree.id, arrayConcat(t.path, [tree.id])
    FROM tree JOIN t ON (tree.parent_id = t.id)
)
SELECT t1.id, t2.path, tuple(t2.*) FROM t AS t1 JOIN t AS t2 ON
(t1.id=t2.id);

-- { echoOff }
