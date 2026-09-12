#!/usr/bin/env bash
# Tags: long, zookeeper, no-fasttest, no-shared-merge-tree
# Tag long: the fixture writes and repeatedly scans a part whose JSON column carries 100000 distinct
#   paths, which takes minutes once several copies of it run at once.
# Tag zookeeper: parts_to_check and the part check thread only exist on a replicated table.
# Tag no-fasttest: the Fast test job runs with --timeout 60, which this fixture exceeds.
# Tag no-shared-merge-tree: the oracle counts ReplicatedMergeTreePartCheckThread log lines, so under
#   --replace-replicated-with-shared it would read 0 whether or not the guard works. Same tag as the
#   other part-check tests (04603, 04604).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./04675_cancelled_json_prefix_read_does_not_report_broken_part.lib
. "$CUR_DIR"/04675_cancelled_json_prefix_read_does_not_report_broken_part.lib

check_cancelled_prefix_read wide
