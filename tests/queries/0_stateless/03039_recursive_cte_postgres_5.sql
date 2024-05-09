
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
-- test multiple WITH queries
--
WITH RECURSIVE
  y AS (SELECT 1 AS id),
  x AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5)
SELECT * FROM x ORDER BY id;

-- forward reference OK
WITH RECURSIVE
    x AS (SELECT * FROM y UNION ALL SELECT id+1 FROM x WHERE id < 5),
    y AS (SELECT 1 AS id)
 SELECT * FROM x ORDER BY id;

WITH RECURSIVE
   x AS
     (SELECT 1 AS id UNION ALL SELECT id+1 FROM x WHERE id < 5),
   y AS
     (SELECT 1 AS id UNION ALL SELECT id+1 FROM y WHERE id < 10)
 SELECT y.*, x.* FROM y LEFT JOIN x USING (id) ORDER BY y.id;

WITH RECURSIVE
   x AS
     (SELECT 1 AS id UNION ALL SELECT id+1 FROM x WHERE id < 5),
   y AS
     (SELECT 1 AS id UNION ALL SELECT id+1 FROM x WHERE id < 10)
 SELECT y.*, x.* FROM y LEFT JOIN x USING (id) ORDER BY y.id;

WITH RECURSIVE
   x AS
     (SELECT 1 AS id UNION ALL SELECT id+1 FROM x WHERE id < 3 ),
   y AS
     (SELECT * FROM x UNION ALL SELECT * FROM x),
   z AS
     (SELECT * FROM x UNION ALL SELECT id+1 FROM z WHERE id < 10)
 SELECT * FROM z ORDER BY id;

WITH RECURSIVE
   x AS
     (SELECT 1 AS id UNION ALL SELECT id+1 FROM x WHERE id < 3 ),
   y AS
     (SELECT * FROM x UNION ALL SELECT * FROM x),
   z AS
     (SELECT * FROM y UNION ALL SELECT id+1 FROM z WHERE id < 10)
 SELECT * FROM z ORDER BY id;

-- { echoOff }
