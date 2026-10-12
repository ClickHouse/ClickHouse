#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory executable UDFs require Linux

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# The command keeps its footprint unchanged by moving pages past the end of the file.
# Scrubbing as another user needs almost twice that footprint and fails the query's limit
# after the old pages have been released. The old user must get a fresh worker afterwards.
cat <<'XML' | shm_functions
<function><type>executable_pool</type><name>shm_hidden_holes</name><return_type>String</return_type>
<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>
<use_shared_memory>1</use_shared_memory><shared_memory_size>33554432</shared_memory_size>
<shared_memory_max_size>134217728</shared_memory_max_size>
<command>shm_udf_region.py --hide-holes</command></function>
XML
shm_local_listening "
    SELECT shm_hidden_holes(1);
    $(shm_as_user other "SELECT shm_hidden_holes(1) SETTINGS max_memory_usage = 50331648, max_untracked_memory = 0")
    SELECT count() FROM shm_regions;
    SELECT shm_hidden_holes(2);
    SELECT count() FROM shm_regions;
"
