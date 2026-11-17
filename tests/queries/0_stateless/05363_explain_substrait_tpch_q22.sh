#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database
# no-fasttest: Substrait is not built in fast test, and the `tpch` database is not created there.
# no-random-settings: the blocking steps depend on which plan optimizations run.
# no-replicated-database: the `tpch` database is not created in DatabaseReplicated mode.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./05363_explain_substrait_tpch.lib
. "$CURDIR"/05363_explain_substrait_tpch.lib

explain_substrait_tpch 22
