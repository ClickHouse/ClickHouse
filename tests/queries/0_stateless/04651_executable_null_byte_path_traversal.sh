#!/usr/bin/env bash
# A NUL byte truncates the path the kernel sees but not the path that gets validated, so a script
# name must not be able to pass the user scripts folder check and still run a file outside of it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$(cd "$(mktemp -d "${CLICKHOUSE_TMP}/exec_traversal_scripts_XXXXXX")" && pwd)
OUTSIDE_DIR=$(cd "$(mktemp -d "${CLICKHOUSE_TMP}/exec_traversal_outside_XXXXXX")" && pwd)
trap 'rm -rf "${SCRIPTS_DIR}" "${OUTSIDE_DIR}"' EXIT

MARKER=EXECUTED_OUTSIDE_USER_SCRIPTS

cat > "${SCRIPTS_DIR}/legit.sh" << 'SCRIPT'
#!/usr/bin/env bash
printf 'ok\n'
SCRIPT
chmod +x "${SCRIPTS_DIR}/legit.sh"

OUTSIDE_SCRIPT="${OUTSIDE_DIR}/outside.sh"
cat > "${OUTSIDE_SCRIPT}" << SCRIPT
#!/usr/bin/env bash
printf '${MARKER}\n'
SCRIPT
chmod +x "${OUTSIDE_SCRIPT}"

CONFIG_FILE="${SCRIPTS_DIR}/local_config.xml"
cat > "${CONFIG_FILE}" << EOF
<clickhouse>
    <user_scripts_path>${SCRIPTS_DIR}/</user_scripts_path>
</clickhouse>
EOF

ups_to_root()
{
    local ups="" path="$1"
    while [ "${path}" != "/" ]; do
        ups="${ups}../"
        path=$(dirname "${path}")
    done
    printf '%s' "${ups}"
}

ESCAPE_REL="$(ups_to_root "${SCRIPTS_DIR}")${OUTSIDE_SCRIPT#/}"

check()
{
    echo -n "$1: "
    $CLICKHOUSE_LOCAL --config-file="${CONFIG_FILE}" \
        --query "SELECT * FROM executable('$2', LineAsString, 'line String')" 2>&1 \
        | grep -aoE "^ok$|${MARKER}|must be inside user scripts folder" || echo UNEXPECTED
}

check 'legit script' 'legit.sh'
check 'null byte in name' 'legit.sh\0x'
check 'null byte escape' "${ESCAPE_REL}\\0/$(ups_to_root "${OUTSIDE_SCRIPT}")${SCRIPTS_DIR#/}/legit.sh"
check 'parent directory' "${ESCAPE_REL}"
