#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A command holds a writable descriptor to its shared-memory region, and the seals stop it only from
# shrinking the file: it can extend the file, commit pages past its end (`fallocate` with
# `FALLOC_FL_KEEP_SIZE`), or free pages inside it (`FALLOC_FL_PUNCH_HOLE`). Whatever it does, the
# server charges what the region holds, holds a pooled worker to `shared_memory_max_size` where the
# region changes hands, and never commits pages that would take the region past that cap itself.
# Each scenario runs in a `clickhouse-local` of its own, so it starts from a process that holds no
# region and no charge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function shm_function()
{
    # name, argument type, the options that make it what it is, the command
    echo "<function><type>executable_pool</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>$2</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory>$3<command>$4</command></function>"
}

{
    # `shm_udf_extend.py` doubles the file on every request - or stretches it to a page, given `page`.
    shm_function shm_extend UInt64 \
        "<shared_memory_size>4194304</shared_memory_size><shared_memory_max_size>67108864</shared_memory_max_size>" shm_udf_extend.py
    shm_function shm_extend_past_cap UInt64 "<shared_memory_size>4194304</shared_memory_size>" shm_udf_extend.py
    shm_function shm_extend_tiny UInt64 "<shared_memory_size>28</shared_memory_size>" "shm_udf_extend.py page"
    # `shm_udf_alloc_beyond_eof.py` commits pages past the end of the file: twice the file's length
    # right past it, or - given an offset - whole pages of the given amount there, after stretching
    # the file to the given length.
    shm_function shm_alloc_beyond_eof UInt64 "<shared_memory_size>1572864</shared_memory_size>" shm_udf_alloc_beyond_eof.py
    shm_function shm_alloc_within_cap UInt64 \
        "<shared_memory_size>40</shared_memory_size><shared_memory_max_size>1835008</shared_memory_max_size>" shm_udf_alloc_beyond_eof.py
    shm_function shm_alloc_far UInt64 \
        "<shared_memory_size>56</shared_memory_size><shared_memory_max_size>1835008</shared_memory_max_size>" "shm_udf_alloc_beyond_eof.py 1048576"
    shm_function shm_alloc_far_filling String \
        "<shared_memory_size>72</shared_memory_size><shared_memory_max_size>1835008</shared_memory_max_size>" "shm_udf_alloc_beyond_eof.py 1572864 262144"
    shm_function shm_alloc_far_during_request String \
        "<shared_memory_size>4096</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size>" "shm_udf_alloc_beyond_eof.py 1048576 512000"
    shm_function shm_sparse UInt64 \
        "<shared_memory_size>88</shared_memory_size><shared_memory_max_size>2097152</shared_memory_max_size>" "shm_udf_alloc_beyond_eof.py 2097152 2093056 2097152"
    shm_function shm_tiny UInt64 "<shared_memory_size>24</shared_memory_size>" shm_udf.py
    shm_function shm_punch_hole UInt64 "<shared_memory_size>1310720</shared_memory_size>" shm_udf_punch_hole.py
    shm_function shm_report_size UInt64 \
        "<shared_memory_size>65536</shared_memory_size><shared_memory_max_size>131072</shared_memory_max_size>" "shm_udf_report_size.py --extend-to 131072"
} | shm_functions

P=$SHM_PAGE

echo "--- an extended region is charged from the next hand-over on"
# The idle charge after the first borrow is the doubled file, and the next borrow is charged the
# doubled file too - so under a limit the configured region would have fitted it is refused, before
# anything reaches the worker. A borrow that can afford it goes through, and the command doubles the
# file again.
shm_local "
    SELECT shm_extend(1);
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions WHERE size = 8388608;
    SELECT shm_extend(2) FORMAT Null SETTINGS max_memory_usage = 6291456, max_untracked_memory = 0;
    SELECT value FROM shm_pooled;
    SELECT shm_extend(3);
    SELECT value FROM shm_pooled;
    SYSTEM RELOAD FUNCTION shm_extend;
    SELECT value FROM shm_pooled;
"

echo "--- a region extended past the cap costs the command its worker"
# The worker is discarded with its region rather than handed back to the pool: nothing of it is
# charged while the slot is idle, and the next query is served by a fresh worker with a fresh region.
shm_local "
    SELECT shm_extend_past_cap(1);
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions;
    SELECT shm_extend_past_cap(2);
    SELECT value FROM shm_pooled;
"
shm_log_contains "past shared_memory_max_size"

echo "--- pages committed past the end of the file count against the cap"
# The cap is on what a region holds, not on how long its file is.
shm_local "
    SELECT shm_alloc_beyond_eof(1);
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions;
    SELECT shm_alloc_beyond_eof(2);
    SELECT value FROM shm_pooled;
"
shm_log_contains "(its length, the pages it committed, or what it would hold once mapped whole), past shared_memory_max_size"

echo "--- a region smaller than a page is not over its own cap"
# A region of 24 bytes holds a page, and its cap defaults to its size. Footprints and caps are
# compared in whole pages, so the same worker - and the same region - serves every call.
shm_local "
    SELECT shm_tiny(1);
    CREATE TABLE first_regions ENGINE = Memory AS SELECT inode FROM shm_regions WHERE size = 24;
    SELECT count() FROM first_regions;
    SELECT shm_tiny(2);
    SELECT shm_tiny(3);
    SELECT (SELECT groupArray(inode) FROM shm_regions WHERE size = 24) = (SELECT groupArray(inode) FROM first_regions);
"

echo "--- pages committed within the cap are charged once"
# The command commits three pages past the end of its 40-byte file. A growth that reaches into them
# commits nothing new, and must not charge them a second time: the idle charge is the footprint of
# the region - the grown length in pages, or the pages the command committed - never both added.
# `sum(length(...))` rather than `count()`: a call whose result nothing needs is optimized out.
shm_local "
    SELECT shm_alloc_within_cap(1);
    CREATE TABLE first_regions ENGINE = Memory AS SELECT inode, committed FROM shm_regions WHERE size = 40;
    SELECT count(), any(committed) >= 3 * $P FROM first_regions;
    SELECT value = (SELECT committed FROM first_regions) FROM shm_pooled;
    SELECT sum(length(shm_alloc_within_cap(number))) FROM numbers(200);
    SELECT count(), any(size) > 40 FROM shm_regions WHERE inode IN (SELECT inode FROM first_regions);
    SELECT value = (SELECT greatest(intDiv(size + $P - 1, $P) * $P, committed) FROM shm_regions WHERE inode IN (SELECT inode FROM first_regions))
    FROM shm_pooled;
"

echo "--- a growth on top of pages committed far past the end is charged for what it commits"
# Three pages a megabyte past the end of a 56-byte file. A growth to a few dozen KiB stops well short
# of them and commits its own pages on top: the footprint says how many pages the file holds, not
# where, and the query is charged exactly the pages the growth added.
shm_local "
    SELECT shm_alloc_far(1);
    CREATE TABLE first_regions ENGINE = Memory AS SELECT inode, committed FROM shm_regions WHERE size = 56;
    SELECT count(), any(committed) = $(shm_pages 56) + 3 * $P FROM first_regions;
    SELECT value = (SELECT committed FROM first_regions) FROM shm_pooled;
    CREATE TABLE allocated_before ENGINE = Memory AS SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryAllocatedBytes';

    SELECT sum(length(shm_alloc_far(number))) FROM numbers(5000);
    CREATE TABLE grown ENGINE = Memory AS SELECT size, committed FROM shm_regions WHERE inode IN (SELECT inode FROM first_regions);
    SELECT count(), any(size) > 56 AND any(size) < 1048576, any(committed) = intDiv(any(size) + $P - 1, $P) * $P + 3 * $P FROM grown;
    SELECT value - (SELECT value FROM allocated_before) = (SELECT committed FROM grown) - (SELECT committed FROM first_regions)
    FROM system.events WHERE event = 'ExecutableUDFSharedMemoryAllocatedBytes';
    SELECT value = (SELECT committed FROM grown) FROM shm_pooled;
"

echo "--- a growth that would take the footprint past the cap is refused before it commits"
# The command commits the last 256 KiB below a cap of 1.75 MiB, far past the end of its 72-byte file.
# One block of input grows the region to 1.125 MiB, and the result then needs a growth that reaches
# into the command's pages: it is refused by the bound on what it could commit at most, before it
# commits anything - not answered with "does not fit" after the server grew first and measured
# later. The worker that filled the region with pages of its own goes, with nothing of it charged.
shm_local "
    SELECT shm_alloc_far_filling('1');
    CREATE TABLE first_regions ENGINE = Memory AS SELECT inode, committed FROM shm_regions WHERE size = 72;
    SELECT count(), any(committed) = $(shm_pages 72) + intDiv(262144, $P) * $P FROM first_regions;
    SELECT sum(length(shm_alloc_far_filling(concat(toString(number), 'xxxxx')))) FROM numbers(65536)
    SETTINGS max_threads = 1, max_block_size = 65536;
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions WHERE inode IN (SELECT inode FROM first_regions);
    SELECT shm_alloc_far_filling('2');
"
shm_log_contains "would take its footprint from"

echo "--- pages committed during a request are seen by the growth it asks for"
# The command commits 500 KB of pages a megabyte past the end while it serves the request, and then
# asks for a region the doubling takes to the cap of 1 MiB. The growth is checked against the
# footprint as it is then, not as it was when the worker was borrowed.
shm_local "
    SELECT length(shm_alloc_far_during_request(repeat('x', 300000)));
    SELECT count() FROM shm_regions;
"
shm_log_contains "The region size requested by the command"
shm_log_contains "past shared_memory_max_size (1048576 bytes)"

echo "--- a file stretched to the cap with pages past its end is discarded before the server fills it in"
# The command stretches its 88-byte file to the cap of 2 MiB without committing a page, and commits
# just under 2 MiB of pages past the end: by its length the file is at the cap, by its pages under
# it. Mapped whole at the next borrow it would hold twice the cap, and that is what is held against
# the cap where the worker is handed back.
shm_local "
    SELECT shm_sparse(1);
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions WHERE size = 2097152;
    SELECT shm_sparse(2);
    SELECT value FROM shm_pooled;
"
shm_log_contains "grown its shared-memory region to $((2097152 + 2093056 / P * P)) bytes (its length, the pages it committed, or what it would hold once mapped whole), past shared_memory_max_size (2097152 bytes); the process will not be reused"

echo "--- the length of the file is held to the cap in bytes"
# Footprints are compared in whole pages, but the length of the file is exact and the command's to
# change: a 28-byte file stretched to a page is stretched past a cap of 28 bytes.
shm_local "
    SELECT shm_extend_tiny(1);
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions;
"
shm_log_contains "past shared_memory_max_size (28 bytes)"

echo "--- a hole punched by the command is not fatal"
# The command frees everything past its answer. The file is as long as it was, a freed page reads as
# zeros and takes a write like any other, so the same worker and region serve the next query.
shm_local "
    SELECT shm_punch_hole(1);
    SELECT count(), any(committed) < 1310720 / 2 FROM shm_regions WHERE size = 1310720;
    SELECT shm_punch_hole(2);
    SELECT count() FROM shm_regions WHERE size = 1310720;
"

echo "--- a file extended by the command is mapped whole at the next borrow"
# The command answers with the size of the file, written at its end, and extends the 64 KiB file to
# 128 KiB after its first answer: the second answer lies past everything the server had mapped.
shm_local "
    SELECT shm_report_size(1);
    SELECT shm_report_size(1);
"
