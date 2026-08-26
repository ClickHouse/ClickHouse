#!/usr/bin/env bash
# The ExecutablePool borrow wait must last max_command_execution_time seconds,
# not ten times that (issue #116467).
#
# Reading the table twice in one query holds the only pool slot across both
# reads, so the second borrow finds the pool full and waits the whole timeout.
# `timeout` is a harness rather than an assertion: a correct conversion gives up
# after 10 seconds and prints the error naming the configured value, while a
# 10x-too-long wait is still blocked when the bound expires and prints nothing.
#
# The elapsed check is a lower bound only. The pool waits on a condition
# variable whose predicate nothing satisfies before the query ends, so the wait
# can overshoot the configured time under load but can never end sooner.
#
# clickhouse-local is required because StorageExecutable accepts only a script
# beneath user_scripts_path, which can be set through a config file alone.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_pool_borrow_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

# send_chunk_header = 1 makes the persistent process read the chunk size first,
# echo it back, then emit exactly that many rows.
cat > "${SCRIPTS_DIR}/echo_pool.sh" << 'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r n; do
    printf '%s\n' "${n}"
    for ((i = 0; i < n; i++)); do
        IFS= read -r id
        printf '%s\n' "${id}"
    done
done
SCRIPT
chmod +x "${SCRIPTS_DIR}/echo_pool.sh"

CONFIG_FILE="${SCRIPTS_DIR}/local_config.xml"
cat > "${CONFIG_FILE}" << EOF
<clickhouse>
    <user_scripts_path>${SCRIPTS_DIR}/</user_scripts_path>
</clickhouse>
EOF

START=$SECONDS
timeout 60 $CLICKHOUSE_LOCAL --config-file="${CONFIG_FILE}" --query "
CREATE TABLE src (id UInt32) ENGINE = Memory;
INSERT INTO src VALUES (1);

CREATE TABLE t_pool (id UInt32)
ENGINE = ExecutablePool('echo_pool.sh', 'TSV', (SELECT id FROM src))
SETTINGS send_chunk_header = 1, pool_size = 1, max_command_execution_time = 10;

SELECT * FROM t_pool UNION ALL SELECT * FROM t_pool;
" < /dev/null 2>&1 | grep -o -F 'Could not get process from pool, max command execution timeout exceeded 10 seconds'

if [ $(( SECONDS - START )) -ge 8 ]; then
    echo 'waited at least the configured time'
else
    echo "returned too early: $(( SECONDS - START ))s"
fi
