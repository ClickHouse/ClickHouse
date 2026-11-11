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

# The client reports the key it has chosen to stderr; the temporary paths and the user name are not stable.
function run_client()
{
    HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file \
        --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed -e "s|${SSH_HOME}|\$HOME|g" -e "s|${USER_NAME}|\$USER|g"
}

echo '--- The default identity file in ~/.ssh'
run_client

echo '--- An IdentityFile with the %r token, the name of the user of the connection'
cp "${SSH_HOME}/.ssh/id_ed25519" "${SSH_HOME}/.ssh/id_${USER_NAME}"
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityFile ~/.ssh/id_%r
EOF
run_client
rm "${SSH_HOME}/.ssh/config" "${SSH_HOME}/.ssh/id_${USER_NAME}"

echo '--- An IdentityFile with an environment variable; an entry with an unset variable is skipped'
mkdir -p "${SSH_HOME}/keys"
cp "${SSH_HOME}/.ssh/id_ed25519" "${SSH_HOME}/keys/work_id"
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityFile ${CLICKHOUSE_TEST_UNSET_KEY_DIR}/id_%r
IdentityFile ${CLICKHOUSE_TEST_KEY_DIR}/work_id
EOF
CLICKHOUSE_TEST_KEY_DIR="${SSH_HOME}/keys" run_client
rm -r "${SSH_HOME}/.ssh/config" "${SSH_HOME}/keys"

echo '--- A configured identity whose key file cannot be imported is skipped in favor of the next one'
echo 'this is not a private key' > "${SSH_HOME}/.ssh/broken_id"
chmod 600 "${SSH_HOME}/.ssh/broken_id"
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityFile ~/.ssh/broken_id
IdentityFile ~/.ssh/id_ed25519
EOF
run_client

echo '--- If none of the identities can be imported, the error lists them'
mkdir "${SSH_HOME}/.ssh/saved"
mv "${SSH_HOME}"/.ssh/id_* "${SSH_HOME}/.ssh/saved/"
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityFile ~/.ssh/broken_id
EOF
run_client | grep -o 'No usable SSH key found: none of these files contains a key that could be used: \$HOME/.ssh/broken_id'
mv "${SSH_HOME}"/.ssh/saved/id_* "${SSH_HOME}/.ssh/"
rmdir "${SSH_HOME}/.ssh/saved"
rm "${SSH_HOME}/.ssh/config" "${SSH_HOME}/.ssh/broken_id"

echo '--- A dead ssh-agent socket does not prevent an explicit key file from being used'
SSH_AUTH_SOCK="${SSH_HOME}/missing-agent.sock" HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file "${SSH_HOME}/.ssh/id_ed25519" \
    --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed "s|${SSH_HOME}|\$HOME|g"

echo '--- A malformed ssh config does not prevent an explicit key file from being used'
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
Match host
    IdentityFile
EOF
HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file "${SSH_HOME}/.ssh/id_ed25519" \
    --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed "s|${SSH_HOME}|\$HOME|g"
rm "${SSH_HOME}/.ssh/config"

cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityAgent ${CLICKHOUSE_TEST_UNSET_AGENT_SOCKET}
EOF
echo '--- An unset IdentityAgent environment variable does not prevent an explicit key file from being used'
env -u CLICKHOUSE_TEST_UNSET_AGENT_SOCKET HOME="${SSH_HOME}" ${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --ssh-key-file "${SSH_HOME}/.ssh/id_ed25519" \
    --query "SELECT currentUser() = '${USER_NAME}'" 2>&1 | sed "s|${SSH_HOME}|\$HOME|g"
rm "${SSH_HOME}/.ssh/config"

eval "$(ssh-agent -s)" > /dev/null
trap 'ssh-agent -k > /dev/null 2>&1' EXIT
ssh-add -q "${SSH_HOME}/.ssh/id_ed25519" 2>/dev/null

echo '--- A malformed public key file does not prevent the private key from being used'
mv "${SSH_HOME}/.ssh/id_ed25519.pub" "${SSH_HOME}/.ssh/id_ed25519.pub.saved"
echo 'ssh-ed25519 this-is-not-base64!' > "${SSH_HOME}/.ssh/id_ed25519.pub"
run_client
mv "${SSH_HOME}/.ssh/id_ed25519.pub.saved" "${SSH_HOME}/.ssh/id_ed25519.pub"

echo '--- A public key file with misplaced base64 padding does not prevent the private key from being used'
mv "${SSH_HOME}/.ssh/id_ed25519.pub" "${SSH_HOME}/.ssh/id_ed25519.pub.saved"
echo 'ssh-ed25519 Zm9vYmF=Zm9v the ed25519 key' > "${SSH_HOME}/.ssh/id_ed25519.pub"
run_client
mv "${SSH_HOME}/.ssh/id_ed25519.pub.saved" "${SSH_HOME}/.ssh/id_ed25519.pub"

# The public key file names the rsa key, which the agent also holds, but the private key next to it is the ed25519 key.
echo '--- A stale public key file does not select a different key from the ssh-agent'
ssh-add -q "${SSH_HOME}/.ssh/id_rsa" 2>/dev/null
mv "${SSH_HOME}/.ssh/id_ed25519.pub" "${SSH_HOME}/.ssh/id_ed25519.pub.saved"
cp "${SSH_HOME}/.ssh/id_rsa.pub" "${SSH_HOME}/.ssh/id_ed25519.pub"
run_client
mv "${SSH_HOME}/.ssh/id_ed25519.pub.saved" "${SSH_HOME}/.ssh/id_ed25519.pub"
ssh-add -qd "${SSH_HOME}/.ssh/id_rsa" 2>/dev/null

# Both keys exist locally, but only the ed25519 key is in the ssh-agent.
echo '--- The first configured identity wins even when only a later one is in the ssh-agent'
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityFile ~/.ssh/id_rsa
IdentityFile ~/.ssh/id_ed25519
EOF
run_client
rm "${SSH_HOME}/.ssh/config"

echo '--- The agent-held copy of the first configured identity is preferred over its file'
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentityFile ~/.ssh/id_ed25519
IdentityFile ~/.ssh/id_rsa
EOF
run_client
rm "${SSH_HOME}/.ssh/config"

echo '--- The ed25519 key in the ssh-agent'
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

echo '--- The rsa key in the ssh-agent, with nothing in ~/.ssh'
run_client

echo '--- IdentitiesOnly yes forbids a key that only the ssh-agent has'
cat > "${SSH_HOME}/.ssh/config" <<'EOF'
IdentitiesOnly yes
EOF
run_client | grep -o 'No SSH key found'
rm "${SSH_HOME}/.ssh/config"

${CLICKHOUSE_CLIENT} --query "DROP USER ${USER_NAME}"
