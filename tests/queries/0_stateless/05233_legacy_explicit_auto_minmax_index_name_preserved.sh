#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# A table created before the implicit min-max indices existed may declare an index of its own that
# happens to be named `auto_minmax_index_<column>`. On load such a name is re-marked as implicitly
# created, so that tables written by older versions - which stored their implicit indices as regular
# ones - keep round-tripping; an index re-marked this way is reported as implicitly created and is
# left out of the table definition that gets written back on the next metadata rewrite.
#
# That re-marking must happen only for a table whose own `SETTINGS` clause states the implicit-index
# policy. The merged storage settings cannot tell: they start from the server defaults, and every
# `<merge_tree>` config override is marked as changed there, so with the policy coming from the
# config the user's own index was re-marked - and then lost on the next metadata rewrite.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/05233_merge_tree_defaults.xml"

cat > "${CONFIG_FILE}" <<XML
<clickhouse>
    <merge_tree>
        <add_minmax_index_for_numeric_columns>1</add_minmax_index_for_numeric_columns>
    </merge_tree>
</clickhouse>
XML

$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" --query "
-- The policy is inherited from the server config, so the index belongs to the user.
ATTACH TABLE t_inherited UUID '10000000-0000-0000-0000-000000000001' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax GRANULARITY 3)
ENGINE = MergeTree ORDER BY tuple();

SELECT 'inherited policy, index stays explicit';
SELECT name, creation, granularity FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_inherited' ORDER BY name;

-- The table states the policy itself, so it was written by a version that stored its implicit
-- indices as regular ones: the index is re-marked as implicit, as before.
ATTACH TABLE t_stored UUID '10000000-0000-0000-0000-000000000002' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax GRANULARITY 3)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

SELECT 'stored policy, index becomes implicit';
SELECT name, creation, granularity FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_stored' ORDER BY name;

DROP TABLE t_inherited;
DROP TABLE t_stored;
"

rm "${CONFIG_FILE}"
