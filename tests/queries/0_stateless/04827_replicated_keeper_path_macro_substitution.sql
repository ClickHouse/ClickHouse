-- Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

SET send_logs_level = 'fatal';
SET database_replicated_allow_replicated_engine_arguments = 1;

-- A {database}/{table} substitution must stay a single ZooKeeper path component.
-- Every path below is scoped by {database} so parallel copies of this test cannot collide.

-- '/' in the substituted value adds path components.
CREATE TABLE `a/b` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r1') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
-- The replica name becomes a znode name under `<path>/replicas/`, so it is a carrier too.
CREATE TABLE `c/d` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/x', '{table}') ORDER BY c0; -- { serverError BAD_ARGUMENTS }

-- '.' and '..' are not valid znode names. This is checked on the expanded path, so the substituted
-- value only has to be illegal once the surrounding template has been applied.
CREATE TABLE `.` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r2') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE `..` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r3') ORDER BY c0; -- { serverError BAD_ARGUMENTS }

-- A configured macro whose value contains {database}/{table} is expanded in the second pass, which
-- must be validated too.
CREATE TABLE `{default_path_test}n1` (c0 Int) ENGINE = ReplicatedMergeTree('{table}', 'r4') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE `{default_path_test}n2/replicas/p` (c0 Int) ENGINE = ReplicatedMergeTree('{table}', 'r5') ORDER BY c0; -- { serverError BAD_ARGUMENTS }

-- Here a configured macro supplies the '/' and the table name supplies the illegal component, so the
-- two appear in different expansion passes.
CREATE TABLE `.` (c0 Int) ENGINE = ReplicatedMergeTree('{default_path_test}{table}', 'r6') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
-- Both macros here are configured ones, so {table} reaches the output only in the second pass: this
-- row is the only one that depends on the substitution being recorded there. The value carries
-- neither '/' nor a brace, so the checks on the value cannot be what rejects it.
CREATE TABLE `i\x01j` (c0 Int) ENGINE = ReplicatedMergeTree('{default_path_test}{default_name_test}', 'r6a') ORDER BY c0; -- { serverError BAD_ARGUMENTS }

-- A closing brace on its own is enough, because it can complete a brace opened by the template.
CREATE TABLE `e}f` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6b') ORDER BY c0; -- { serverError BAD_ARGUMENTS }

-- Control characters are not valid znode names either. U+0085 is a C1 control, encoded as C2 85.
CREATE TABLE `g\x01h` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6c') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE `g\xC2\x85h` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6d') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
-- Both ends of each forbidden range: U+007F is a single byte, U+0080 and U+009F need the pair test.
CREATE TABLE `g\x7Fh` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6d1') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE `g\xC2\x80h` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6d2') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE `g\xC2\x9Fh` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6d3') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
-- A name whose bytes merely look like the above is unaffected: U+00BF is C2 BF, outside the C1 range.
CREATE TABLE `g\xC2\xBFh` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6e') ORDER BY c0;
CREATE TABLE `таблица_🚀_表` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r6f') ORDER BY c0;

-- Ordinary and dotted names keep working: the rule is not a character-level ban on '.'.
CREATE TABLE t_plain (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r7') ORDER BY c0;
CREATE TABLE `my.table` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', 'r8') ORDER BY c0;

-- An embedded substitution still yields a legal component, so it must be accepted.
CREATE TABLE `.` (c0 Int) ENGINE = ReplicatedMergeTree('{default_path_test}embedded', '{default_name_test}') ORDER BY c0;
SELECT splitByChar('/', replica_path)[-1] FROM system.replicas WHERE database = currentDatabase() AND table = '.';

-- The replica name is a znode name too, so an illegal component there is rejected the same way. This
-- value has no '/', so only the component check on the replica name can reject it.
CREATE TABLE `..` (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/x', '{table}') ORDER BY c0; -- { serverError BAD_ARGUMENTS }
-- An explicitly written value is judged on its own merits even when the path substitutes: the same '.'
-- that is rejected above when it comes from {table} is accepted here.
CREATE TABLE t_explicit_replica (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/{table}', '.') ORDER BY c0;
SELECT replica_name FROM system.replicas WHERE database = currentDatabase() AND table = 't_explicit_replica';

-- Paths written out in full are untouched, including the nesting an operator may rely on.
CREATE TABLE t_nested_path (c0 Int) ENGINE = ReplicatedMergeTree('/clickhouse/04827/{database}/a/b/c', 'r9') ORDER BY c0;
SELECT replaceOne(zookeeper_path, currentDatabase(), '{db}') FROM system.replicas WHERE database = currentDatabase() AND table = 't_nested_path';

-- A definition read back from metadata is never re-judged, so an existing table keeps loading.
DETACH TABLE t_plain;
ATTACH TABLE t_plain;

DROP TABLE t_plain;
DROP TABLE `my.table`;
DROP TABLE `.`;
DROP TABLE `g\xC2\xBFh`;
DROP TABLE `таблица_🚀_表`;
DROP TABLE t_explicit_replica;
DROP TABLE t_nested_path;
