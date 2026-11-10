#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory executable UDFs require Linux

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

cat <<'XML' | shm_functions
<function><type>executable_pool</type><name>shm_echo</name><return_type>String</return_type>
<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>
<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>
<command>shm_udf.py</command></function>
XML
# A completed answer remains valid, but a worker with an unknown footprint cannot be pooled.
shm_local "
    SYSTEM ENABLE FAILPOINT executable_udf_fail_handback_measurement;
    SELECT shm_echo(1);
    SELECT count() FROM shm_regions;
    SELECT shm_echo(2);
    SELECT count() FROM shm_regions;
"
