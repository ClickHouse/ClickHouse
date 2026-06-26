-- Row policy interaction between Merge table and child tables
-- Covers NOT_FOUND_COLUMN_IN_BLOCK when parent's row_level_filter drops columns that the child's policy still needs
-- See: https://github.com/ClickHouse/ClickHouse/issues/102650

-- ===== Setup =====

DROP ROW POLICY IF EXISTS rp_c1 ON child1;
DROP ROW POLICY IF EXISTS rp_c2 ON child2;
DROP ROW POLICY IF EXISTS rp_m ON m;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS child2;
DROP TABLE IF EXISTS child1;

CREATE TABLE child1 (id Int64, val Float64, is_valid Bool DEFAULT true, category String DEFAULT 'A')
ENGINE = MergeTree ORDER BY id;

CREATE TABLE child2 (id Int64, val Float64, is_valid Bool DEFAULT true, category String DEFAULT 'A')
ENGINE = MergeTree ORDER BY id;

INSERT INTO child1 VALUES (1, 10, true, 'A'), (2, 20, false, 'A'), (3, 30, true, 'B'), (4, 40, false, 'B');
INSERT INTO child2 VALUES (5, 50, true, 'A'), (6, 60, false, 'A'), (7, 70, true, 'B'), (8, 80, false, 'B');

-- Single child: Merge table matches only child1
CREATE TABLE m AS child1 ENGINE = Merge(currentDatabase(), '^child1$');
CREATE VIEW v AS SELECT id, argMax(val, id) AS val FROM m GROUP BY id;


-- ===== 1. One child, same RP on same column =====
SELECT '--- 1. one child, same rp same column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING is_valid = true TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- ===== 2. One child, RP on different columns =====
SELECT '--- 2. one child, rp on different columns';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING category = 'A' TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- ===== 3. One child, different RPs on same column =====
SELECT '--- 3. one child, different rps same column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING NOT (is_valid = false) TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- ===== 4. One child, self-excluding RPs on same column =====
SELECT '--- 4. one child, self-excluding rps';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING is_valid = false TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- Switch to multi-child Merge table
-- Drop policies before dropping the table, otherwise they become orphaned
DROP ROW POLICY IF EXISTS rp_m ON m;
DROP ROW POLICY IF EXISTS rp_c1 ON child1;
DROP ROW POLICY IF EXISTS rp_c2 ON child2;
DROP VIEW v;
DROP TABLE m;
CREATE TABLE m AS child1 ENGINE = Merge(currentDatabase(), '^child[12]$');
CREATE VIEW v AS SELECT id, argMax(val, id) AS val FROM m GROUP BY id;


-- ===== 5. Multi child, same RP on same column =====
SELECT '--- 5. multi child, same rp same column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_c2 ON child2 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING is_valid = true TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_c2 ON child2;
DROP ROW POLICY rp_m ON m;


-- ===== 6. Multi child, RP on different columns =====
SELECT '--- 6. multi child, rp on different columns';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_c2 ON child2 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING category = 'A' TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_c2 ON child2;
DROP ROW POLICY rp_m ON m;


-- ===== 7. Multi child, different RPs on same column =====
SELECT '--- 7. multi child, different rps same column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_c2 ON child2 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING NOT (is_valid = false) TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_c2 ON child2;
DROP ROW POLICY rp_m ON m;


-- ===== 8. Multi child, self-excluding RPs =====
SELECT '--- 8. multi child, self-excluding rps';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_c2 ON child2 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING is_valid = false TO ALL;
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_c2 ON child2;
DROP ROW POLICY rp_m ON m;


-- ===== 9. Multi child, one child has RP, merge different column =====
SELECT '--- 9. multi child, one child rp, merge different column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING category = 'A' TO ALL;
-- child1: is_valid=true AND category='A' → id=1
-- child2: no child rp, category='A' → id=5,6
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- ===== 10. Multi child, one child has RP, merge same rp same column =====
SELECT '--- 10. multi child, one child rp, merge same column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING is_valid = true TO ALL;
-- child1: is_valid=true (both policies) → id=1,3
-- child2: is_valid=true (merge only) → id=5,7
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- ===== 11. Multi child, one child has RP, merge different rp same column =====
SELECT '--- 11. multi child, one child rp, merge different rp same column';
CREATE ROW POLICY rp_c1 ON child1 FOR SELECT USING is_valid = true TO ALL;
CREATE ROW POLICY rp_m ON m FOR SELECT USING is_valid = false TO ALL;
-- child1: is_valid=true AND is_valid=false → nothing
-- child2: is_valid=false (merge only) → id=6,8
SELECT id, val FROM v WHERE val > 0 ORDER BY id;
DROP ROW POLICY rp_c1 ON child1;
DROP ROW POLICY rp_m ON m;


-- ===== Cleanup =====
DROP VIEW v;
DROP TABLE m;
DROP TABLE child2;
DROP TABLE child1;
