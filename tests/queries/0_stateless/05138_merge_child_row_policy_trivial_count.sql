-- A row policy on a child of a Merge table must be applied to count() as well, whether or not
-- the trivial count optimization is allowed to serve the count from metadata.
DROP ROW POLICY IF EXISTS mcp ON mc_child1;
DROP TABLE IF EXISTS mc_merge;
DROP TABLE IF EXISTS mc_child1;
DROP TABLE IF EXISTS mc_child2;

CREATE TABLE mc_child1 (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mc_child2 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO mc_child1 VALUES (1), (2), (3);
INSERT INTO mc_child2 VALUES (4), (5), (6);
CREATE TABLE mc_merge (id UInt32) ENGINE = Merge(currentDatabase(), '^mc_child[12]$');

CREATE ROW POLICY mcp ON mc_child1 FOR SELECT USING id = 1 TO CURRENT_USER;

SELECT 'rows visible through Merge', arraySort(groupArray(id)) FROM mc_merge;
SELECT 'count, trivial count disabled', count() FROM mc_merge SETTINGS optimize_trivial_count_query = 0;
SELECT 'count, trivial count enabled', count() FROM mc_merge SETTINGS optimize_trivial_count_query = 1;
SELECT 'count, sparsity filter enabled', count() FROM mc_merge WHERE id != 0
    SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1;

DROP ROW POLICY mcp ON mc_child1;
DROP TABLE mc_merge;
DROP TABLE mc_child1;
DROP TABLE mc_child2;
