#!/usr/bin/env bash
# Defining an `Executable` table must not run its script, and a closed `allow_executable_tables`
# must refuse a read before spawning the process. The script appends to a marker file, so any
# execution is observable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_gate_scripts_XXXXXX")
trap 'rm -rf "${SCRIPTS_DIR}"' EXIT

MARKER="${SCRIPTS_DIR}/executed"

cat > "${SCRIPTS_DIR}/marker.sh" << SCRIPT
#!/usr/bin/env bash
echo ran >> '${MARKER}'
printf '1\n'
SCRIPT
chmod +x "${SCRIPTS_DIR}/marker.sh"

CONFIG_FILE="${SCRIPTS_DIR}/local_config.xml"
cat > "${CONFIG_FILE}" << CONFIG
<clickhouse>
    <user_scripts_path>${SCRIPTS_DIR}/</user_scripts_path>
</clickhouse>
CONFIG

# Gate closed: the definition is accepted and the refused read spawns nothing.
$CLICKHOUSE_LOCAL --config-file="${CONFIG_FILE}" --query "
SET allow_executable_tables = 0;
CREATE TABLE t_gate (x UInt32) ENGINE = Executable('marker.sh', 'TSV');
SELECT * FROM t_gate;
" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'

# Gate open: defining and wrapping still run nothing.
$CLICKHOUSE_LOCAL --config-file="${CONFIG_FILE}" --query "
CREATE TABLE t_gate (x UInt32) ENGINE = Executable('marker.sh', 'TSV');
CREATE VIEW v_gate AS SELECT * FROM t_gate;
"
if [ -e "${MARKER}" ]; then echo "script ran"; else echo "script did not run"; fi

# Positive control: proves the marker is observable, so the check above is not vacuous.
$CLICKHOUSE_LOCAL --config-file="${CONFIG_FILE}" --query "
CREATE TABLE t_gate (x UInt32) ENGINE = Executable('marker.sh', 'TSV');
SELECT * FROM t_gate;
"
if [ -e "${MARKER}" ]; then echo "script ran"; else echo "script did not run"; fi
