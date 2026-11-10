#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: pipe resizing and the blocked-writer probe require Linux.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# A two-page pipe is the kernel default after the per-user pipe budget is exceeded. On a machine
# with 4 KiB pages, reading the capped diagnostic and the remaining page takes 4 KiB each. Both
# reads must count toward deciding whether a writer can still be finishing a blocked burst.
GO="$SHM_UDF_WORK/go"
BURST_BYTES=$((8 * $(getconf PAGESIZE)))

shm_functions <<EOF
<function>
    <type>executable_pool</type>
    <name>small_stderr_pipe</name>
    <argument><type>UInt64</type></argument>
    <return_type>UInt64</return_type>
    <format>TabSeparated</format>
    <pool_size>1</pool_size>
    <stderr_reaction>log_last</stderr_reaction>
    <command_read_timeout>5000</command_read_timeout>
    <command>pipe_pool_pid.py --go $GO --after-go stderr-flood --pipe-pages 2 --bytes $BURST_BYTES</command>
</function>
EOF

# The next statement starts only once the previous invocation's stderr writer is blocked.
# Finishing the burst must let the same worker answer the next request.
shm_local "
    CREATE TABLE pids (pid UInt64) ENGINE = Memory;
    INSERT INTO pids SELECT small_stderr_pipe(0);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT any(pid) FROM pids));
    INSERT INTO pids SELECT small_stderr_pipe(1);
    SELECT count(), uniqExact(pid) FROM pids;
"
