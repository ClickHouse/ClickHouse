#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The shared-memory options of an executable UDF are checked when its configuration is loaded: a
# function with an invalid combination is never created, the loader says why, and the functions
# next to it in the same file are not affected. What a function was configured with is visible in
# `system.user_defined_functions`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function shm_function()
{
    # name, type, the options that make it what it is
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>"
    echo "$3<command>shm_udf.py</command></function>"
}

{
    shm_function shm_ok executable "<use_shared_memory>1</use_shared_memory><shared_memory_size>1048576</shared_memory_size>"
    shm_function shm_grow executable \
        "<use_shared_memory>1</use_shared_memory><shared_memory_size>16</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size>"
    shm_function shm_pool executable_pool \
        "<use_shared_memory>1</use_shared_memory><shared_memory_size>1048576</shared_memory_size>"

    shm_function bad_chunk_header executable \
        "<use_shared_memory>1</use_shared_memory><shared_memory_size>1048576</shared_memory_size><send_chunk_header>1</send_chunk_header>"
    # Every shared-memory-only key is refused without `use_shared_memory`: accepting it would let a
    # function that was explicitly configured for shared memory run over the pipes instead. The
    # rejection is on the key being present, not on its value - a knob written out at its own
    # default says just as clearly that its author believed the function used shared memory.
    shm_function bad_size_no_shm executable "<shared_memory_size>1048576</shared_memory_size>"
    shm_function bad_max_size_default_no_shm executable "<shared_memory_max_size>0</shared_memory_max_size>"
    shm_function bad_max_size_no_shm executable "<shared_memory_max_size>1048576</shared_memory_max_size>"
    shm_function bad_max_lt_size executable \
        "<use_shared_memory>1</use_shared_memory><shared_memory_size>1048576</shared_memory_size><shared_memory_max_size>524288</shared_memory_max_size>"
    # The size is the one thing the transport cannot default: a missing one and an explicit zero are
    # both a region of nothing.
    shm_function bad_no_size executable "<use_shared_memory>1</use_shared_memory>"
    shm_function bad_zero_size executable "<use_shared_memory>1</use_shared_memory><shared_memory_size>0</shared_memory_size>"
    # Past the signed range (`Int64`, `off_t`): it must never reach the memory tracker or `ftruncate`.
    shm_function bad_huge executable \
        "<use_shared_memory>1</use_shared_memory><shared_memory_size>18446744073709551615</shared_memory_size>"
    # The largest size the signed range admits - but a file holds whole pages, and the charge for it
    # is the next page boundary, one past the signed range.
    shm_function bad_int64_max executable \
        "<use_shared_memory>1</use_shared_memory><shared_memory_size>9223372036854775807</shared_memory_size>"
} | shm_functions

shm_local "
    SELECT shm_ok(1);

    -- A function the loader refused does not exist at all: using it is not some runtime failure
    -- that happens to mention its name.
    SELECT bad_chunk_header(1);
    SELECT bad_int64_max(1);

    -- Why each one was refused.
    SELECT name, load_status,
        multiIf(
            loading_error_message LIKE '%\`use_shared_memory\` is incompatible with \`send_chunk_header\`%', 'chunk header',
            loading_error_message LIKE '%\`shared_memory_size\` requires \`use_shared_memory\`%', 'size without shm',
            loading_error_message LIKE '%\`shared_memory_max_size\` requires \`use_shared_memory\`%', 'max size without shm',
            loading_error_message LIKE '%\`shared_memory_max_size\` (524288) must not be smaller%', 'max size below size',
            loading_error_message LIKE '%\`shared_memory_size\` must be greater than zero%', 'no size',
            loading_error_message LIKE '%\`shared_memory_size\` (18446744073709551615) must not exceed%', 'past Int64',
            loading_error_message LIKE '%shared-memory charge (up to 9223372036854775808 bytes, rounded up to whole pages) must not exceed 9223372036854775807%', 'Int64 max in pages',
            loading_error_message)
    FROM system.user_defined_functions WHERE name LIKE 'bad\\_%' ORDER BY name;

    -- What the transport is configured with is answerable from SQL. A region that may grow reports
    -- the bound it may grow to, not the raw \`0\` the configuration uses for \"it may not\"; a function
    -- the loader refused has no configuration at all, so its columns are at their defaults.
    SELECT name, load_status, use_shared_memory, shared_memory_size, shared_memory_max_size
    FROM system.user_defined_functions WHERE name IN ('shm_ok', 'shm_grow', 'shm_pool', 'bad_size_no_shm') ORDER BY name;
"
