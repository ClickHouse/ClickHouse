#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# The read-only implicit-index policy of a table need not be stored in its `SETTINGS`: it can be
# inherited from the server's `<merge_tree>` config section (or `compatibility`). A settings-only
# ALTER that reopens a gate, such as `RESET SETTING enable_block_number_column` on a table created
# with `enable_block_number_column = 0`, must recompute the implicit indices on top of those
# inherited defaults. Before the fix the recompute started from the compiled-in defaults, so the
# inherited `add_minmax_index_for_block_number_column = 1` was lost and the running table did not
# regain `auto_minmax_index__block_number` until DETACH / ATTACH or restart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/05218_merge_tree_defaults.xml"

cat > "${CONFIG_FILE}" <<XML
<clickhouse>
    <merge_tree>
        <add_minmax_index_for_block_number_column>1</add_minmax_index_for_block_number_column>
        <enable_block_number_column>1</enable_block_number_column>
        <add_minmax_index_for_numeric_columns>0</add_minmax_index_for_numeric_columns>
    </merge_tree>
</clickhouse>
XML

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --query "
CREATE TABLE t_inherited_implicit_index (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0;

SELECT 'created with the gate closed', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_inherited_implicit_index';

ALTER TABLE t_inherited_implicit_index RESET SETTING enable_block_number_column;

SELECT 'live after RESET SETTING', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_inherited_implicit_index' ORDER BY name;

ALTER TABLE t_inherited_implicit_index MODIFY SETTING enable_block_number_column = 0;

SELECT 'live after closing the gate again', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_inherited_implicit_index';

ALTER TABLE t_inherited_implicit_index MODIFY SETTING enable_block_number_column = DEFAULT;

SELECT 'live after = DEFAULT', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_inherited_implicit_index' ORDER BY name;

INSERT INTO t_inherited_implicit_index SELECT number, number FROM numbers(10);
INSERT INTO t_inherited_implicit_index SELECT number, number FROM numbers(10);
OPTIMIZE TABLE t_inherited_implicit_index FINAL;

SELECT 'merged part carries the index', name, secondary_indices_marks_bytes > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_inherited_implicit_index' AND active ORDER BY name;

DROP TABLE t_inherited_implicit_index;
"

rm "${CONFIG_FILE}"
