#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mutations run with the global context settings, so `use_constant_folding_in_index_analysis` cannot be
# set from a client session; in `clickhouse-local` the query context is the global one.
$CLICKHOUSE_LOCAL --use_constant_folding_in_index_analysis=1 -m -q "
CREATE TABLE t_mutation_composite_partition (a UInt64, b UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY (a, b) ORDER BY tuple();

INSERT INTO t_mutation_composite_partition VALUES (1, 2, 0);
INSERT INTO t_mutation_composite_partition VALUES (3, 4, 0);

-- \`a + b\` mixes two partition key columns, so \`KeyCondition\` cannot decompose it and the partition
-- pruner keeps both parts. Only the minmax condition specialized for the part's partition value folds
-- it to a constant, so both parts are proven untouched here.
ALTER TABLE t_mutation_composite_partition UPDATE v = 1 WHERE a + b = 100 SETTINGS mutations_sync = 2;

SELECT sum(v), count() FROM t_mutation_composite_partition;
SELECT value FROM system.events WHERE event = 'MutationUntouchedPartsByIndexAnalysis';
"
