#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Credentials in the URL query parameters take precedence over the HTTP `Authorization`
# header: when both are present the header is ignored, instead of the request being
# rejected for mixing authentication methods. A browser that has remembered Basic
# credentials attaches the header to every request automatically, while the Web UI
# (`play.html`) authenticates by putting the user name and password into URL parameters.

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"

# A bogus `Authorization` header (as a browser would send after remembering some other
# credentials) together with the correct credentials in the URL parameters. The query
# must succeed and run as the user from the parameters; without the precedence it would
# fail with "not allowed to use Authorization HTTP header and authentication via
# parameters simultaneously".
bogus=$(echo -n 'remembered_user:remembered_password' | base64)

${CLICKHOUSE_CURL} -sS \
    -H "Authorization: Basic ${bogus}" \
    "${CLICKHOUSE_URL}&user=${user}&password=password" \
    --data-binary "SELECT currentUser() = '${user}'"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
