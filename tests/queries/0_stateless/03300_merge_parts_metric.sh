#!/usr/bin/env bash
# Tags: long, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This test verifies that the MergeParts metric correctly tracks the number of
# source parts in active merges.
#
# Due to inherent timing issues (the metric and system.merges cannot be read atomically),
# we use a statistical approach: run many comparisons and verify that the values match
# in at least some percentage of cases.
#
# The comparison can fail when:
# 1. A merge starts between reading the metric and reading system.merges (metric higher)
# 2. A merge ends between reading the metric and reading system.merges (metric lower)
#
# We sample the metric before and after reading system.merges, and check if the sum
# falls within that range. This should significantly reduce false negatives.

yes "WITH metric_before AS (SELECT value FROM system.metrics WHERE name = 'MergeParts'), merges_sum AS (SELECT sum(length(source_part_names)) AS total FROM system.merges), metric_after AS (SELECT value FROM system.metrics WHERE name = 'MergeParts') SELECT merges_sum.total BETWEEN least(metric_before.value, metric_after.value) AND greatest(metric_before.value, metric_after.value) FROM metric_before, merges_sum, metric_after;" | head -n1000 | $CLICKHOUSE_CLIENT | {
  sort | uniq -cd | awk '$1 > 100 && $NF == "1" { print $NF }'
}
