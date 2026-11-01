#!/usr/bin/env bash
# Test coverage for StorageExecutable::readImpl() and transformToSingleBlockSources().
# Uses clickhouse-local with a custom user_scripts_path so no server-side script
# installation is required.  user_scripts_path is a server config option, so it
# must be passed via --config-file rather than as a bare CLI argument.
#
# ExecutablePool requires send_chunk_header=1 so the persistent process knows
# when each chunk ends.  The script must echo the chunk size back before the
# processed rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_storage_scripts_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

# ---------------------------------------------------------------------------
# Script 1: plain generator — outputs two rows, no stdin consumed.
# ---------------------------------------------------------------------------
cat > "${SCRIPTS_DIR}/gen_rows.sh" << 'SCRIPT'
#!/usr/bin/env bash
printf '1\thello\n'
printf '2\tworld\n'
SCRIPT
chmod +x "${SCRIPTS_DIR}/gen_rows.sh"

# ---------------------------------------------------------------------------
# Script 2: transform — reads key\tval from stdin, emits key\tupper(val).
# Used with an input_query to cover the inputs loop in readImpl.
# ---------------------------------------------------------------------------
cat > "${SCRIPTS_DIR}/uppercase.sh" << 'SCRIPT'
#!/usr/bin/env bash
while IFS=$'\t' read -r key val; do
    printf '%s\t%s\n' "${key}" "${val^^}"
done
SCRIPT
chmod +x "${SCRIPTS_DIR}/uppercase.sh"

# ---------------------------------------------------------------------------
# Script 3: chunk-header-aware transform for ExecutablePool.
# Reads the chunk size, echoes it, then processes exactly that many rows.
# Loops so the persistent pool process handles multiple queries.
# ---------------------------------------------------------------------------
cat > "${SCRIPTS_DIR}/uppercase_pool.sh" << 'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r n; do
    printf '%s\n' "${n}"
    for ((i = 0; i < n; i++)); do
        IFS=$'\t' read -r key val
        printf '%s\t%s\n' "${key}" "${val^^}"
    done
done
SCRIPT
chmod +x "${SCRIPTS_DIR}/uppercase_pool.sh"

# Build a minimal config that sets user_scripts_path for this session.
CONFIG_FILE="${SCRIPTS_DIR}/local_config.xml"
cat > "${CONFIG_FILE}" << EOF
<clickhouse>
    <user_scripts_path>${SCRIPTS_DIR}/</user_scripts_path>
</clickhouse>
EOF

# ---------------------------------------------------------------------------
# Test 1: Executable with no input query — covers the basic readImpl path.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL \
    --config-file="${CONFIG_FILE}" \
    --query "
CREATE TABLE t_exec_basic (id UInt32, val String)
ENGINE = Executable('gen_rows.sh', 'TSV')
SETTINGS send_chunk_header = 0, check_exit_code = 1;

SELECT * FROM t_exec_basic ORDER BY id;
"

# ---------------------------------------------------------------------------
# Test 2: Executable with an input_query — covers the inputs loop and the
#          InterpreterSelectQueryAnalyzer branch inside readImpl.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL \
    --config-file="${CONFIG_FILE}" \
    --query "
CREATE TABLE src (id UInt32, val String) ENGINE = Memory;
INSERT INTO src VALUES (10, 'alpha'), (20, 'beta');

CREATE TABLE t_exec_input (id UInt32, val String)
ENGINE = Executable('uppercase.sh', 'TSV', (SELECT id, val FROM src ORDER BY id));

SELECT * FROM t_exec_input ORDER BY id;
"

# ---------------------------------------------------------------------------
# Test 3: ExecutablePool with an input_query — covers transformToSingleBlockSources
#          and the is_executable_pool branch.  send_chunk_header=1 is required so
#          the persistent process knows when each chunk ends.
# ---------------------------------------------------------------------------
$CLICKHOUSE_LOCAL \
    --config-file="${CONFIG_FILE}" \
    --query "
CREATE TABLE src (id UInt32, val String) ENGINE = Memory;
INSERT INTO src VALUES (10, 'alpha'), (20, 'beta');

CREATE TABLE t_exec_pool (id UInt32, val String)
ENGINE = ExecutablePool('uppercase_pool.sh', 'TSV', (SELECT id, val FROM src ORDER BY id))
SETTINGS send_chunk_header = 1, pool_size = 1;

SELECT * FROM t_exec_pool ORDER BY id;
"
