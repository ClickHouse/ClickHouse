#!/usr/bin/env bash
# a column that a row policy mentions cannot be dropped, renamed or get a new type, everything else stays allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
UDF="${DB}_udf"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t (id UInt32, tenant UInt32, note String, extra UInt8, misc UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 1, 'a', 0, 0), (2, 2, 'b', 0, 0), (3, 1, 'c', 0, 0);
CREATE ROW POLICY p ON $DB.t FOR SELECT USING tenant = 1 AND length(note) > 0 TO CURRENT_USER;
SELECT 'before', arraySort(groupArray(id)) FROM t;

-- a column the policy mentions cannot be dropped or renamed
ALTER TABLE t DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t RENAME COLUMN note TO comment; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- other columns are free
ALTER TABLE t DROP COLUMN extra;
ALTER TABLE t RENAME COLUMN misc TO misc2;
SELECT 'after', arraySort(groupArray(id)) FROM t;

-- a new type of a column the policy mentions is refused, the same type and CLEAR are allowed
ALTER TABLE t MODIFY COLUMN tenant String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t MODIFY COLUMN tenant UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t MODIFY COLUMN tenant UInt32 CODEC(ZSTD);
SELECT 'after modify', arraySort(groupArray(id)) FROM t;
ALTER TABLE t CLEAR COLUMN tenant;
SELECT 'after clear', arraySort(groupArray(id)) FROM t;
INSERT INTO t (id, tenant, note) VALUES (4, 1, 'd');
SELECT 'after reinsert', arraySort(groupArray(id)) FROM t;

-- IF EXISTS on a missing column stays a no-op even if a stale policy mentions it
CREATE ROW POLICY pstale ON $DB.t FOR SELECT USING gone = 1 TO CURRENT_USER;
ALTER TABLE t DROP COLUMN IF EXISTS gone;
ALTER TABLE t RENAME COLUMN IF EXISTS gone TO still_gone;
DROP ROW POLICY pstale ON $DB.t;

-- the names inside a subquery belong to the tables the subquery reads
CREATE TABLE allowed (tenant UInt32) ENGINE = MergeTree ORDER BY tenant;
INSERT INTO allowed VALUES (1);
CREATE TABLE t2 (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, 1, 0), (2, 2, 0);
CREATE ROW POLICY psub ON $DB.t2 FOR SELECT USING id IN (SELECT tenant FROM $DB.allowed) TO CURRENT_USER;
ALTER TABLE t2 DROP COLUMN id; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t2 RENAME COLUMN tenant TO tenant_id;
SELECT 'subquery policy after rename', arraySort(groupArray(id)) FROM t2;

-- table-qualified names count
CREATE TABLE tq (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pq ON $DB.tq FOR SELECT USING tq.tenant = 1 AND $DB.tq.tenant = 1 TO CURRENT_USER;
ALTER TABLE tq DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tq RENAME COLUMN tenant TO tenant_id; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tq RENAME COLUMN other TO other2;

-- a subcolumn counts as a mention of its column
CREATE TABLE tj (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pj ON $DB.tj FOR SELECT USING j.user.name = 'a' TO CURRENT_USER;
ALTER TABLE tj DROP COLUMN j; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tj RENAME COLUMN j TO data; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- so does a Nested child
CREATE TABLE tn (id UInt32, n Nested(x UInt32, y UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pn ON $DB.tn FOR SELECT USING n.x[1] = 1 TO CURRENT_USER;
ALTER TABLE tn DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tn RENAME COLUMN n.x TO n.z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tn DROP COLUMN n.y;

-- a column mentioned inside a SQL function body
CREATE FUNCTION $UDF AS (x) -> x = tenant;
CREATE TABLE tu (id UInt32, tenant UInt32, other UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pu ON $DB.tu FOR SELECT USING $UDF(1) TO CURRENT_USER;
ALTER TABLE tu DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tu RENAME COLUMN tenant TO tenant_id; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tu RENAME COLUMN other TO other2;

-- so is a SQL function passed by name as a lambda
CREATE TABLE tf (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY pf ON $DB.tf FOR SELECT USING arrayExists($UDF, [1]) TO CURRENT_USER;
ALTER TABLE tf DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tf RENAME COLUMN tenant TO tenant_id; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a column with the name of a called SQL function is not read
CREATE TABLE tun (id UInt32, tenant UInt32, $UDF UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO tun VALUES (1, 1, 0), (2, 2, 0);
CREATE ROW POLICY pun ON $DB.tun FOR SELECT USING $UDF(1) TO CURRENT_USER;
ALTER TABLE tun DROP COLUMN $UDF;
SELECT 'function name policy after drop', arraySort(groupArray(id)) FROM tun;
ALTER TABLE tun DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a lambda parameter is not a column, the body is still read
CREATE TABLE tlam (id UInt32, x UInt8, tenant UInt32, arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tlam VALUES (1, 0, 1, [1]), (2, 0, 2, [2]);
CREATE ROW POLICY plam ON $DB.tlam FOR SELECT USING arrayExists(x -> x = 1, arr) AND $UDF(id) TO CURRENT_USER;
ALTER TABLE tlam DROP COLUMN x;
SELECT 'lambda policy after drop', arraySort(groupArray(id)) FROM tlam;
ALTER TABLE tlam DROP COLUMN arr; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tlam RENAME COLUMN tenant TO tenant_id; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a name binds as written first, exact column or subcolumn, a table name or an alias is removed first with analyzer_compatibility_prefer_alias_over_subcolumn, both count
CREATE TABLE tt (id UInt32, x UInt8, tt Tuple(x UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tt VALUES (1, 1, (0)), (2, 0, (1));
CREATE ROW POLICY ptt ON $DB.tt FOR SELECT USING tt.x = 1 TO CURRENT_USER;
CREATE TABLE tl (id UInt32, x UInt8, \`tl.x\` UInt8, \`$DB.tl.x\` UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ptl ON $DB.tl FOR SELECT USING tl.x = 1 AND $DB.tl.x = 1 TO CURRENT_USER;
CREATE TABLE tl2 (id UInt32, x UInt8, m UInt8, \`$DB.tl2.x\` UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO tl2 VALUES (1, 0, 0, 1), (2, 0, 0, 2);
CREATE ROW POLICY ptl2 ON $DB.tl2 FOR SELECT USING $DB.tl2.x = 1 TO CURRENT_USER;
SELECT 'subcolumn first', arraySort(groupArray(id)) FROM tt;
SELECT 'column first', arraySort(groupArray(id)) FROM tt SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
ALTER TABLE tt DROP COLUMN tt; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tt DROP COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tl RENAME COLUMN \`tl.x\` TO \`tl.y\`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tl DROP COLUMN \`$DB.tl.x\`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tl DROP COLUMN x; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tl2 DROP COLUMN x;
ALTER TABLE tl2 RENAME COLUMN m TO $DB;
SELECT 'exact qualified column policy after alters', arraySort(groupArray(id)) FROM tl2;

-- a column cannot get a name that a policy reads as another column, the table name included
CREATE TABLE tr (id UInt32, tenant UInt8, payload Tuple(tenant String), misc UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO tr VALUES (1, 1, ('no'), 0), (2, 2, ('yes'), 0);
CREATE ROW POLICY ptr ON $DB.tr FOR SELECT USING tr.tenant = 1 TO CURRENT_USER;
ALTER TABLE tr RENAME COLUMN payload TO tr; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tr RENAME COLUMN payload TO payload2;
ALTER TABLE tr RENAME COLUMN misc TO misc2;
SELECT 'qualifier policy after rename', arraySort(groupArray(id)) FROM tr;

-- a column of its own with a dot in the name is not a subcolumn of the shorter name
CREATE TABLE td (id UInt32, a UInt32, \`a.b\` UInt32, m UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO td VALUES (1, 0, 1, 0), (2, 0, 2, 0);
CREATE ROW POLICY pd ON $DB.td FOR SELECT USING \`a.b\` = 1 TO CURRENT_USER;
ALTER TABLE td RENAME COLUMN a TO a2;
ALTER TABLE td DROP COLUMN a2;
ALTER TABLE td RENAME COLUMN m TO a;
SELECT 'dotted column policy after alters', arraySort(groupArray(id)) FROM td;
ALTER TABLE td DROP COLUMN \`a.b\`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a subquery over a same-named table of another database reads that table
CREATE DATABASE ${DB}_2;
CREATE TABLE ${DB}_2.ts (tenant UInt32) ENGINE = MergeTree ORDER BY tenant;
CREATE TABLE ts (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY ps ON $DB.ts FOR SELECT USING id IN (SELECT tenant FROM ${DB}_2.ts) TO CURRENT_USER;
ALTER TABLE ts RENAME COLUMN tenant TO tenant_id;

-- a column named like a SQL function is the column, the function body is not read
CREATE TABLE tsame (id UInt32, $UDF UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY psame ON $DB.tsame FOR SELECT USING $UDF = 1 TO CURRENT_USER;
ALTER TABLE tsame DROP COLUMN tenant;
ALTER TABLE tsame DROP COLUMN $UDF; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a read through an Alias applies the policies of the alias and of its target
CREATE TABLE tal (id UInt32, tenant UInt32, note String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al ENGINE = Alias(tal);
CREATE ROW POLICY ptal ON $DB.tal FOR SELECT USING tenant = 1 TO CURRENT_USER;
CREATE ROW POLICY pal ON $DB.al FOR SELECT USING length(al.note) > 0 TO CURRENT_USER;
ALTER TABLE al DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal RENAME COLUMN note TO comment; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a policy of an Alias can read the target in a subquery, those names count
CREATE TABLE tal2 (id UInt32, tenant UInt32, note String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al2 ENGINE = Alias(tal2);
CREATE ROW POLICY pal2 ON $DB.al2 FOR SELECT USING id IN (SELECT id FROM tal2 WHERE tenant = 1) TO CURRENT_USER;
ALTER TABLE tal2 DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal2 RENAME COLUMN note TO comment;

-- a name in that subquery can have the qualifier of the target or of its alias
CREATE TABLE tal3 (id UInt32, tenant UInt32, note String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE al3 ENGINE = Alias(tal3);
CREATE ROW POLICY pal3 ON $DB.al3 FOR SELECT USING id IN (SELECT q.id FROM tal3 AS q WHERE q.tenant = 1)
    AND id IN (SELECT id FROM tal3 WHERE length(tal3.note) > 0) TO CURRENT_USER;
ALTER TABLE tal3 DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal3 RENAME COLUMN note TO comment; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- the names of a subquery count only when it, or a subquery around it, reads the target
CREATE TABLE tal4 (id UInt32, tenant UInt32, note String, extra UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal4 VALUES (1, 1, 'a', 1), (2, 2, 'b', 1), (3, 1, 'c', 5);
CREATE TABLE other4 (id UInt32, note String) ENGINE = MergeTree ORDER BY id;
INSERT INTO other4 VALUES (1, 'x'), (2, 'y'), (3, 'z');
CREATE TABLE al4 ENGINE = Alias(tal4);
CREATE ROW POLICY pal4 ON $DB.al4 FOR SELECT USING id IN (SELECT id FROM $DB.other4 WHERE note != '' AND id IN (SELECT q.id FROM $DB.tal4 AS q WHERE q.tenant = 1))
    AND id IN (SELECT q.id FROM $DB.tal4 AS q WHERE EXISTS (SELECT 1 FROM $DB.other4 AS o WHERE o.id = q.extra)) TO CURRENT_USER;
ALTER TABLE tal4 DROP COLUMN note;
ALTER TABLE tal4 DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal4 DROP COLUMN extra; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'nested subquery policy after drop', arraySort(groupArray(id)) FROM al4 SETTINGS enable_parallel_replicas = 0;

-- a derived table or a CTE that reads the target qualifies the names of the target with its own name
CREATE TABLE tal5 (id UInt32, tenant UInt32, note String, extra UInt32, flag UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal5 VALUES (1, 1, 'a', 1, 1, 0), (2, 2, 'b', 1, 1, 0), (3, 1, 'c', 1, 1, 0);
CREATE TABLE other5 (id UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO other5 VALUES (1, 1), (2, 1), (3, 1);
CREATE TABLE al5 ENGINE = Alias(tal5);
CREATE ROW POLICY pal5 ON $DB.al5 FOR SELECT USING id IN (SELECT q.id FROM (SELECT * FROM $DB.tal5) AS q WHERE q.tenant = 1)
    AND id IN (WITH c AS (SELECT * FROM $DB.tal5) SELECT c.id FROM c WHERE length(c.note) > 0)
    AND id IN (WITH c AS (SELECT * FROM $DB.tal5) SELECT r.id FROM c AS r JOIN (SELECT * FROM $DB.other5) AS o ON r.id = o.id WHERE r.extra = 1 AND o.misc = 1)
    AND id IN (WITH c AS (SELECT * FROM $DB.tal5) SELECT s.id FROM (SELECT * FROM c) AS s WHERE s.flag = 1) TO CURRENT_USER;
ALTER TABLE tal5 DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal5 RENAME COLUMN note TO comment; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal5 DROP COLUMN extra; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal5 DROP COLUMN flag; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal5 DROP COLUMN misc;
SELECT 'derived table policy after drop', arraySort(groupArray(id)) FROM al5 SETTINGS enable_parallel_replicas = 0;

-- so does a table function that reads the target
CREATE TABLE tal6 (id UInt32, tenant UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal6 VALUES (1, 1, 0), (2, 2, 0), (3, 1, 0);
CREATE TABLE al6 ENGINE = Alias(tal6);
CREATE ROW POLICY pal6 ON $DB.al6 FOR SELECT USING id IN (SELECT q.id FROM view(SELECT * FROM $DB.tal6) AS q WHERE q.tenant = 1) TO CURRENT_USER;
ALTER TABLE tal6 DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal6 DROP COLUMN misc;
SELECT 'table function policy after drop', arraySort(groupArray(id)) FROM al6 SETTINGS enable_parallel_replicas = 0;

-- a SQL function can read the target in a subquery inside a subquery over another table
CREATE TABLE tal9 (id UInt32, tenant UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal9 VALUES (1, 1, 0), (2, 2, 0);
CREATE TABLE al9 ENGINE = Alias(tal9);
CREATE FUNCTION ${UDF}_max AS () -> (SELECT max(tenant) FROM $DB.tal9);
CREATE ROW POLICY pal9 ON $DB.al9 FOR SELECT USING id IN (SELECT ${UDF}_max() FROM numbers(1)) TO CURRENT_USER;
ALTER TABLE tal9 DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal9 DROP COLUMN misc;
SELECT 'function in subquery policy after drop', arraySort(groupArray(id)) FROM al9 SETTINGS enable_parallel_replicas = 0;

-- a CTE over another table hides the target of the same name
CREATE TABLE tal10 (id UInt32, tenant UInt32, flag UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal10 VALUES (1, 1, 1), (2, 2, 1);
CREATE TABLE other10 (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO other10 VALUES (1, 0), (2, 1);
CREATE TABLE al10 ENGINE = Alias(tal10);
CREATE ROW POLICY pal10 ON $DB.al10 FOR SELECT USING id IN (WITH tal10 AS (SELECT * FROM $DB.other10) SELECT id FROM tal10 WHERE tenant = 1)
    AND id IN (SELECT id FROM tal10 WHERE flag = 1) TO CURRENT_USER;
ALTER TABLE tal10 DROP COLUMN flag; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal10 RENAME COLUMN tenant TO tenant_id;
SELECT 'hidden target policy after rename', arraySort(groupArray(id)) FROM al10 SETTINGS enable_parallel_replicas = 0;

-- a qualifier binds in its own subquery, an alias of another table there hides the policy table and the outer aliases
CREATE TABLE tal7 (id UInt32, tenant UInt32, flag UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal7 VALUES (1, 1, 1), (2, 2, 1), (3, 1, 1);
CREATE TABLE other7 (id UInt32, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO other7 VALUES (1, 1), (2, 1), (3, 2);
CREATE TABLE al7 ENGINE = Alias(tal7);
CREATE ROW POLICY pal7 ON $DB.al7 FOR SELECT USING id IN (SELECT r.id FROM $DB.tal7 AS r INNER JOIN $DB.other7 AS al7 ON r.id = al7.id WHERE al7.tenant = 1 AND r.flag = 1)
    AND id IN (SELECT q.id FROM $DB.tal7 AS q WHERE q.id IN (SELECT q.id FROM $DB.other7 AS q WHERE q.tenant = 1)) TO CURRENT_USER;
ALTER TABLE tal7 DROP COLUMN flag; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal7 RENAME COLUMN tenant TO tenant_id;
SELECT 'scoped alias policy after rename', arraySort(groupArray(id)) FROM al7 SETTINGS enable_parallel_replicas = 0;

-- a CTE binds in its own subquery and in the subqueries inside, where a CTE of the same name hides it, and it can read a later CTE
CREATE TABLE tal8 (id UInt32, tenant UInt32, flag UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tal8 VALUES (1, 1, 1, 0), (2, 2, 1, 0), (3, 1, 1, 0);
CREATE TABLE c8 (id UInt32, tenant UInt32, misc UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO c8 VALUES (1, 1, 1), (2, 1, 1), (3, 2, 1);
CREATE TABLE al8 ENGINE = Alias(tal8);
CREATE ROW POLICY pal8 ON $DB.al8 FOR SELECT USING id IN (WITH c8 AS (SELECT * FROM $DB.tal8) SELECT c8.id FROM c8)
    AND id IN (SELECT id FROM c8 WHERE tenant = 1)
    AND id IN (WITH c8 AS (SELECT * FROM $DB.tal8) SELECT id FROM (WITH c8 AS (SELECT * FROM $DB.c8) SELECT id FROM c8 WHERE misc = 1))
    AND id IN (WITH x AS (SELECT * FROM y), y AS (SELECT * FROM $DB.tal8) SELECT id FROM x WHERE flag = 1) TO CURRENT_USER;
ALTER TABLE tal8 DROP COLUMN flag; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tal8 RENAME COLUMN tenant TO tenant_id;
ALTER TABLE tal8 DROP COLUMN misc;
SELECT 'scoped CTE policy after rename and drop', arraySort(groupArray(id)) FROM al8 SETTINGS enable_parallel_replicas = 0;

-- a table qualifier that is also a column name is removed when the subcolumn does not exist
CREATE TABLE tc (id UInt32, tc UInt8, tenant UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tc VALUES (1, 0, 1), (2, 0, 2);
CREATE ROW POLICY pc ON $DB.tc FOR SELECT USING tc.tenant = 1 TO CURRENT_USER;
ALTER TABLE tc DROP COLUMN tenant; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tc DROP COLUMN tc;
SELECT 'qualifier column policy after drop', arraySort(groupArray(id)) FROM tc;

-- a flattened Nested group reads all of its children
CREATE TABLE tng (id UInt32, n Nested(x UInt32, y UInt32), other UInt8) ENGINE = MergeTree ORDER BY id;
CREATE ROW POLICY png ON $DB.tng FOR SELECT USING notEmpty(n) TO CURRENT_USER;
ALTER TABLE tng DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tng DROP COLUMN n.y; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tng RENAME COLUMN n.x TO n.z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tng DROP COLUMN other;

-- a column read as a subcolumn, through a function or next to a scalar subquery cannot get a new type
CREATE TABLE tm (id UInt32, tp Tuple(a UInt8, b UInt8), n Nullable(UInt8), k UInt32, u Tuple(a UInt8, b UInt8), free UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO tm VALUES (1, (1, 1), 1, 1, (1, 1), 0), (2, (1, 2), 2, 1, (1, 2), 0);
CREATE ROW POLICY pm ON $DB.tm FOR SELECT USING tm.tp.b = 1 AND NOT n.null AND k = 1 AND (SELECT 1) = 1 AND tupleElement(u, 'b') = 1 TO CURRENT_USER;
ALTER TABLE tm MODIFY COLUMN tp Tuple(a UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tm MODIFY COLUMN tp Tuple(a UInt8, c UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tm MODIFY COLUMN tp Tuple(a UInt8, b UInt8, c UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tm MODIFY COLUMN n UInt8 DEFAULT 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tm MODIFY COLUMN k String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tm MODIFY COLUMN u Tuple(a UInt8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tm MODIFY COLUMN n COMMENT 'kept';
ALTER TABLE tm MODIFY COLUMN free String;
SELECT 'modified columns policy', arraySort(groupArray(id)) FROM tm;

-- the policy pins an ALIAS column, the alias pins its base column
CREATE TABLE ta (id UInt32, tenant UInt32, tenant_alias UInt32 ALIAS tenant) ENGINE = MergeTree ORDER BY id;
INSERT INTO ta VALUES (1, 1), (2, 2);
CREATE ROW POLICY pa ON $DB.ta FOR SELECT USING tenant_alias = 1 TO CURRENT_USER;
ALTER TABLE ta DROP COLUMN tenant_alias; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE ta DROP COLUMN tenant; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE ta RENAME COLUMN tenant TO tenant2;
SELECT 'alias policy after base rename', arraySort(groupArray(id)) FROM ta;

-- an ALIAS column the policy reads pins the type of its base columns
CREATE TABLE tac (id UInt32, n UInt32, a UInt8 ALIAS n = 1, b UInt8 ALIAS a) ENGINE = MergeTree ORDER BY id;
INSERT INTO tac (id, n) VALUES (1, 1), (2, 2);
CREATE ROW POLICY pac ON $DB.tac FOR SELECT USING b TO CURRENT_USER;
ALTER TABLE tac MODIFY COLUMN n String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE tac MODIFY COLUMN n UInt32 CODEC(ZSTD);
SELECT 'alias over a modified column', arraySort(groupArray(id)) FROM tac;

-- a database-wide policy counts too
CREATE ROW POLICY pdb ON $DB.* FOR SELECT USING tenant = 1 TO CURRENT_USER;
ALTER TABLE t RENAME COLUMN tenant TO tenant2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP ROW POLICY
    pdb ON $DB.*, p ON $DB.t, psub ON $DB.t2, pq ON $DB.tq, pj ON $DB.tj, pn ON $DB.tn,
    pu ON $DB.tu, pa ON $DB.ta, pac ON $DB.tac, pf ON $DB.tf, pun ON $DB.tun, plam ON $DB.tlam,
    ptt ON $DB.tt, ptl ON $DB.tl, ptl2 ON $DB.tl2, ptr ON $DB.tr, pd ON $DB.td, ps ON $DB.ts,
    psame ON $DB.tsame, ptal ON $DB.tal, pal ON $DB.al, pal2 ON $DB.al2, pal3 ON $DB.al3, pal4 ON $DB.al4,
    pal5 ON $DB.al5, pal6 ON $DB.al6, pal7 ON $DB.al7, pal8 ON $DB.al8, pal9 ON $DB.al9, pal10 ON $DB.al10,
    pc ON $DB.tc, png ON $DB.tng, pm ON $DB.tm;
DROP FUNCTION $UDF;
DROP FUNCTION ${UDF}_max;
DROP DATABASE ${DB}_2;
"
