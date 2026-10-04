#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The control protocol of the shared-memory transport of an executable UDF: requests and responses,
# a region that grows for the input or at the command's request, and every way a command can answer
# wrongly - too few or too many rows, an error status, an offset out of the region, no answer at
# all. Each scenario runs in a `clickhouse-local` of its own, so it starts from a process that holds
# no region and no worker.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function shm_function()
{
    # name, type, the options that make it what it is, the command, the arguments (one `UInt64` if
    # not given), the format (`TabSeparated` if not given)
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "${5-<argument><type>UInt64</type></argument>}<format>${6:-TabSeparated}</format>"
    echo "<use_shared_memory>1</use_shared_memory>$3<command>$4</command></function>"
}

{
    shm_function shm executable "<shared_memory_size>1048576</shared_memory_size>" shm_udf.py
    shm_function shm_pool executable_pool "<shared_memory_size>1048576</shared_memory_size>" shm_udf.py
    shm_function shm_zero_arg executable "<shared_memory_size>1048576</shared_memory_size>" "shm_udf.py --zero-argument" ""
    shm_function shm_zero_arg_pool executable_pool \
        "<pool_size>1</pool_size><shared_memory_size>1048576</shared_memory_size>" "shm_udf.py --zero-argument" ""
    # `shm_udf_grow.py` echoes its input back at offset 0; `shm_udf.py` writes its result after it.
    shm_function shm_grow executable "<shared_memory_size>16</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size>" shm_udf_grow.py
    shm_function shm_grow_pool executable_pool \
        "<shared_memory_size>16</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size>" shm_udf_grow.py
    shm_function shm_grow_after_input executable \
        "<shared_memory_size>16</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size>" shm_udf.py
    shm_function shm_exact executable "<shared_memory_size>6</shared_memory_size>" shm_udf_grow.py
    shm_function shm_binary_pool executable_pool "<pool_size>2</pool_size><shared_memory_size>1048576</shared_memory_size>" shm_udf_binary.py \
        "<argument><type>UInt64</type><name>id</name></argument><argument><type>Nullable(String)</type><name>label</name></argument>" RowBinary

    shm_function shm_pool_short executable_pool "<shared_memory_size>1048576</shared_memory_size>" shm_udf_short.py
    shm_function shm_pool_over executable_pool "<shared_memory_size>1048576</shared_memory_size>" shm_udf_over.py
    shm_function shm_small executable "<shared_memory_size>8</shared_memory_size>" shm_udf.py
    shm_function shm_tiny executable "<shared_memory_size>4</shared_memory_size>" shm_udf.py
    shm_function shm_error executable "<shared_memory_size>1048576</shared_memory_size>" shm_udf_error.py
    shm_function shm_pool_error executable_pool "<shared_memory_size>4096</shared_memory_size>" shm_udf_error.py
    shm_function shm_bad_offset executable "<shared_memory_size>1048576</shared_memory_size>" shm_udf_bad_offset.py
    shm_function shm_die executable "<shared_memory_size>1048576</shared_memory_size>" shm_udf_die.py
    shm_function shm_pool_die executable_pool "<pool_size>2</pool_size><shared_memory_size>4096</shared_memory_size>" shm_udf_die.py
} | shm_functions

echo "--- requests and responses"
# A non-pooled `shm_udf.py` exits only after stdin EOF, so its stdin has to be closed before the
# wait for it once the rows are read. A pooled one is borrowed again and again with its region.
shm_local "
    SELECT shm(1) SETTINGS max_execution_time = 5;
    SELECT shm(number) FROM numbers(3);
    SELECT shm_pool(1); SELECT shm_pool(2); SELECT shm_pool(3);
    SELECT shm_pool(number) FROM numbers(4);
"

echo "--- a function without arguments is still called"
# Its input block has no columns and no rows, so there is nothing to serialize - the request carries
# an empty payload, as the pipe transport does. Pooled, one worker and one region answer every call.
# The request does not say how many rows it is made for, so over a block of three rows the command
# answers one and the query fails - as over the pipes; the next call is answered as before.
shm_local "
    SELECT shm_zero_arg();
    SELECT shm_zero_arg_pool(); SELECT shm_zero_arg_pool(); SELECT shm_zero_arg_pool();
    SELECT count() FROM shm_regions;
    SELECT shm_zero_arg_pool() FROM numbers(3);
    SELECT shm_zero_arg_pool();
"

echo "--- the region grows for the input and at the command's request"
# A region of 16 bytes grows for the serialized input. `shm_udf.py` finds no room for its result
# after the input, asks for a larger region through the protocol, and gets it. Pooled, the region
# keeps its grown size. Input that fills the region to its very last byte does not need it to grow.
shm_local "
    SELECT countIf(shm_grow(number) = toString(number)) FROM numbers(200);
    SELECT countIf(shm_grow_after_input(number) = 'Key ' || toString(number)) FROM numbers(200);
    SELECT countIf(shm_grow_pool(number) = toString(number)) FROM numbers(200);
    SELECT countIf(shm_grow_pool(number) = toString(number)) FROM numbers(200);
    SELECT shm_exact(number) FROM numbers(3);
"

echo "--- a binary format round-trips"
# Unlike a line-oriented text format, `RowBinary` has no give that could absorb a byte lost or
# gained: a `Nullable` with its own null map, embedded NUL bytes and newlines all have to survive.
shm_local "
    SELECT shm_binary_pool(id, label)
    FROM values('id UInt64, label Nullable(String)',
        (1, 'plain'), (2, NULL), (3, 'with\\0embedded\\0nuls'), (4, 'with\\nnewline'), (5, ''))
    ORDER BY id
    FORMAT TSV;
    SELECT shm_binary_pool(7, 'again');
"

echo "--- too few and too many rows"
# Too many rows are caught before the oversized chunk leaves the source, and the worker that produced
# them is discarded with its region: the next borrow fails the same way, and the pool still works.
shm_local "
    SELECT shm_pool_short(number) FROM numbers(3);
    SELECT shm_pool_over(number) FROM numbers(3) FORMAT Null;
    SELECT shm_pool_over(number) FROM numbers(3) FORMAT Null;
    SELECT count() FROM shm_regions;
    SELECT shm_pool(number) FROM numbers(3);
"
shm_log_contains "wrong result, expected 3 row(s), actual 1"
shm_log_contains "wrong result, expected 3 row(s), but the command produced more"

echo "--- input or result that does not fit"
shm_local "
    SELECT shm_small(number) FROM numbers(1000) FORMAT Null;
    SELECT shm_tiny(1) FORMAT Null;
"
shm_log_contains "The serialized input (at least"
shm_log_contains "The region size requested by the command"
shm_log_contains "increase shared_memory_max_size"

echo "--- the command reports an error"
# An error status is a report, not a protocol violation: the response was read in full, so a pooled
# worker survives it with its region - one region was ever created.
shm_local "
    SELECT shm_error(1) FORMAT Null;
    SELECT shm_pool_error(1) FORMAT Null;
    SELECT shm_pool_error(1) FORMAT Null;
    SELECT shm_pool_error(1) FORMAT Null;
    SELECT count() FROM shm_regions;
    SELECT value = $(shm_pages 1048576) + $(shm_pages 4096) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryAllocatedBytes';
"
shm_log_contains "the command cannot process this request"

echo "--- the command answers with an offset out of the region"
shm_local "
    SELECT shm_bad_offset(1) FORMAT Null;
"
shm_log_contains "out-of-bounds region"

echo "--- the command dies without answering"
# Every such borrow fails at once, drops the dead worker with its region and gives the slot back, so
# more failures than \`pool_size\` do not start timing out on the pool.
shm_local "
    SELECT shm_die(1) FORMAT Null;
    SELECT shm_pool_die(1) FORMAT Null;
    SELECT shm_pool_die(1) FORMAT Null;
    SELECT shm_pool_die(1) FORMAT Null;
    SELECT count() FROM shm_regions;
    SELECT shm(1);
"
shm_log_contains "Could not get process from pool"
