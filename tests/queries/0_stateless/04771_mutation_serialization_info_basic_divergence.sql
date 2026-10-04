-- A mutation of a Compact part must not depend on whether the source part's serialization info
-- was still in memory or reloaded from `serialization.json`. `serialization_info_version = 'basic'`
-- cannot persist `propagate_types_serialization_versions_to_nested_types`, which used to make the
-- two settings objects compare unequal and emit info entries for columns that were never written.
-- `arr` is the carrier: Array cannot use sparse serialization, so it gets no entry in the source part.

DROP TABLE IF EXISTS t_in_memory SYNC;
DROP TABLE IF EXISTS t_reloaded SYNC;

CREATE TABLE t_in_memory (cleared String, arr Array(String) DEFAULT []) ENGINE = MergeTree ORDER BY tuple()
SETTINGS serialization_info_version = 'basic',
         propagate_types_serialization_versions_to_nested_types = 1,
         min_bytes_for_wide_part = 1000000000;

CREATE TABLE t_reloaded (cleared String, arr Array(String) DEFAULT []) ENGINE = MergeTree ORDER BY tuple()
SETTINGS serialization_info_version = 'basic',
         propagate_types_serialization_versions_to_nested_types = 1,
         min_bytes_for_wide_part = 1000000000;

INSERT INTO t_in_memory (cleared) VALUES ('x');
INSERT INTO t_reloaded (cleared) VALUES ('x');

-- The only difference between the two tables: this one rebuilds its serialization info from disk.
DETACH TABLE t_reloaded;
ATTACH TABLE t_reloaded;

ALTER TABLE t_in_memory CLEAR COLUMN cleared SETTINGS mutations_sync = 2;
ALTER TABLE t_reloaded CLEAR COLUMN cleared SETTINGS mutations_sync = 2;

-- The pins have to hold or the assertions below measure nothing: the divergence needs a Compact part.
SELECT 'part types', groupArray(part_type) FROM (SELECT part_type FROM system.parts
    WHERE database = currentDatabase() AND table IN ('t_in_memory', 't_reloaded') AND active ORDER BY table);

-- The witness.
SELECT 'mutated parts identical',
    (SELECT bytes_on_disk FROM system.parts WHERE database = currentDatabase() AND table = 't_in_memory' AND active)
  = (SELECT bytes_on_disk FROM system.parts WHERE database = currentDatabase() AND table = 't_reloaded' AND active);

SELECT 'mutated part hashes identical',
    (SELECT (hash_of_all_files, hash_of_uncompressed_files) FROM system.parts WHERE database = currentDatabase() AND table = 't_in_memory' AND active)
  = (SELECT (hash_of_all_files, hash_of_uncompressed_files) FROM system.parts WHERE database = currentDatabase() AND table = 't_reloaded' AND active);

-- The control: the divergence was metadata-only, so this stays green before and after the fix.
SELECT 'reads agree',
    (SELECT groupArray((cleared, arr)) FROM t_in_memory) = (SELECT groupArray((cleared, arr)) FROM t_reloaded);

-- Without this the rows above are all satisfied by two un-mutated parts, which are identical anyway.
SELECT 'cleared values', (SELECT groupArray(cleared) FROM t_in_memory), (SELECT groupArray(cleared) FROM t_reloaded);

DROP TABLE t_in_memory SYNC;
DROP TABLE t_reloaded SYNC;
