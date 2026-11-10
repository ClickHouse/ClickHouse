#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

for function_type in executable executable_pool; do
    echo "--- $function_type"
    shm_functions <<EOF
<function>
    <type>$function_type</type><name>shm_sampler</name>
    <argument><type>UInt64</type></argument><return_type>String</return_type>
    <format>TabSeparated</format><command>shm_udf.py</command>
    <use_shared_memory>1</use_shared_memory><pool_size>1</pool_size>
    <shared_memory_size>4096</shared_memory_size><shared_memory_max_size>65536</shared_memory_max_size>
</function>
EOF

    # Each invocation serializes 3890 input bytes and receives 7890 output bytes. The input fits
    # the initial region, but the output needs a growth response and a retry. Generic byte
    # accounting must include the payload once, plus the small control frames on both attempts.
    # The second invocation also covers the sampler attached to a reused pooled worker.
    shm_local "
        SELECT countIf(shm_sampler(number) = 'Key ' || toString(number)) = 1000
        FROM numbers(1000) SETTINGS max_block_size = 1000, max_threads = 1;
        SELECT countIf(shm_sampler(number) = 'Key ' || toString(number)) = 1000
        FROM numbers(1000) SETTINGS max_block_size = 1000, max_threads = 1;
        SELECT
            sumIf(value, event = 'ExecutableUDFSharedMemoryInputBytes') = 7780,
            sumIf(value, event = 'ExecutableUDFSharedMemoryOutputBytes') = 15780,
            sumIf(value, event = 'ExecutableUDFSharedMemoryRegionGrowths') > 0,
            sumIf(value, event = 'ExecutableUserDefinedFunctionInputBytes') BETWEEN 7781 AND 8036,
            sumIf(value, event = 'ExecutableUserDefinedFunctionOutputBytes') BETWEEN 15781 AND 16036
        FROM system.events;
    "
done
