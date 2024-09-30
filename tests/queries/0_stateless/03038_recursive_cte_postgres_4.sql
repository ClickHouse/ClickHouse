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
-- test cycle detection
--

DROP TABLE IF EXISTS graph;
CREATE TABLE graph(
    f UInt64,
    t UInt64,
    label String
)
ENGINE = TinyLog;

INSERT INTO graph VALUES (1, 2, 'arc 1 -> 2'), (1, 3, 'arc 1 -> 3'), (2, 3, 'arc 2 -> 3'), (1, 4, 'arc 1 -> 4'), (4, 5, 'arc 4 -> 5'), (5, 1, 'arc 5 -> 1');

WITH RECURSIVE search_graph AS (
	SELECT *, false AS is_cycle, [tuple(g.f, g.t)] AS path FROM graph g
	UNION ALL
	SELECT g.*, has(path, tuple(g.f, g.t)), arrayConcat(sg.path, [tuple(g.f, g.t)])
	FROM graph g, search_graph sg
	WHERE g.f = sg.t AND NOT is_cycle
)
SELECT * FROM search_graph
SETTINGS query_plan_join_inner_table_selection = 'right'
;

-- ordering by the path column has same effect as SEARCH DEPTH FIRST
WITH RECURSIVE search_graph AS (
	SELECT *, false AS is_cycle, [tuple(g.f, g.t)] AS path FROM graph g
	UNION ALL
	SELECT g.*, has(path, tuple(g.f, g.t)), arrayConcat(sg.path, [tuple(g.f, g.t)])
	FROM graph g, search_graph sg
	WHERE g.f = sg.t AND NOT is_cycle
)
SELECT * FROM search_graph ORDER BY path;

-- { echoOff }
