#!/usr/bin/env bash
# Tags: no-fasttest, no-openssl-fips
# no-openssl-fips: authenticates with an ssh-ed25519 key, which is not FIPS-approved and is rejected on FIPS builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--ssh-key-file` without a value: the key is looked up in `~/.ssh` and in the ssh-agent.

unset SSH_AUTH_SOCK

SSH_HOME="${CLICKHOUSE_TMP}/home_${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${SSH_HOME}/.ssh"
chmod 700 "${SSH_HOME}/.ssh"

ssh-keygen -q -t ed25519 -N '' -C 'the ed25519 key' -f "${SSH_HOME}/.ssh/id_ed25519"
ssh-keygen -q -t rsa -b 2048 -N '' -C 'the rsa key' -f "${SSH_HOME}/.ssh/id_rsa"

USER_NAME="ssh_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${USER_NAME};
    CREATE USER ${USER_NAME} IDENTIFIED WITH ssh_key
        BY KEY '$(awk '{print $2}' "${SSH_HOME}/.ssh/id_ed25519.pub")' TYPE 'ssh-ed25519',
           KEY '$(awk '{print $2}' "${SSH_HOME}/.ssh/id_rsa.pub")' TYPE 'ssh-rsa';
    GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};
"

# The client reports the key it has chosen to stderr; the temporary paths in the report are not stable.
function run_client()
{
    HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file \
        --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed "s|${SSH_HOME}|\$HOME|g"
}

echo '--- The default identity file in ~/.ssh'
run_client

echo '--- A dead ssh-agent socket does not prevent an explicit key file from being used'
SSH_AUTH_SOCK="${SSH_HOME}/missing-agent.sock" HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file "${SSH_HOME}/.ssh/id_ed25519" \
    --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed "s|${SSH_HOME}|\$HOME|g"

cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityAgent ${CLICKHOUSE_TEST_UNSET_AGENT_SOCKET}
EOF
echo '--- An unset IdentityAgent environment variable does not prevent an explicit key file from being used'
env -u CLICKHOUSE_TEST_UNSET_AGENT_SOCKET HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file "${SSH_HOME}/.ssh/id_ed25519" \
    --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed "s|${SSH_HOME}|\$HOME|g"
rm "${SSH_HOME}/.ssh/config"

echo '--- The ed25519 key in the ssh-agent'
eval "$(ssh-agent -s)" > /dev/null
trap 'ssh-agent -k > /dev/null 2>&1' EXIT
ssh-add -q "${SSH_HOME}/.ssh/id_ed25519" 2>/dev/null
# Only the agent has the private key now, but the public key file still tells which one it is.
rm "${SSH_HOME}/.ssh/id_ed25519"
run_client

echo '--- A global IdentityAgent expands an environment variable'
AGENT_SOCKET="${SSH_AUTH_SOCK}"
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityAgent ${CLICKHOUSE_TEST_AGENT_SOCKET}
EOF
CLICKHOUSE_TEST_AGENT_SOCKET="${AGENT_SOCKET}" SSH_AUTH_SOCK="${SSH_HOME}/missing-agent.sock" run_client
rm "${SSH_HOME}/.ssh/config"

echo '--- The rsa key in the ssh-agent, with nothing in ~/.ssh'
ssh-add -qD 2>/dev/null
ssh-add -q "${SSH_HOME}/.ssh/id_rsa" 2>/dev/null
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
Match host localhost
    IdentityAgent none
Host *
    IdentityAgent SSH_AUTH_SOCK
EOF
echo '--- A matching IdentityAgent none disables an available ssh-agent'
run_client
rm "${SSH_HOME}/.ssh/config"
rm "${SSH_HOME}/.ssh/id_ed25519.pub" "${SSH_HOME}/.ssh/id_rsa" "${SSH_HOME}/.ssh/id_rsa.pub"
run_client

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER_NAME}"
