#!/usr/bin/env bash

# A `Filesystem` database caches every table it resolves, keyed on the table name alone, and hands
# the cached storage back to every later caller. Resolving a table of such a database requires the
# read source grant, and requires nothing that a `file` table function call written in a query
# would require, on both the cached and the uncached path.
# https://github.com/ClickHouse/ClickHouse/issues/118042

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05076_${CLICKHOUSE_DATABASE}_$RANDOM"
fs_db="db_05076_${CLICKHOUSE_DATABASE}_$RANDOM"
dir="${USER_FILES_PATH}/d_05076_${CLICKHOUSE_DATABASE}_$RANDOM"

mkdir -p "$dir"
echo 7 > "$dir/warm.csv"
echo 7 > "$dir/cold.csv"

# The source grant is the variable under test, so the table grant is deliberately broad and cannot
# confound the result. It also shows that it does not confer the source grant by itself.
${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS $fs_db;
DROP USER IF EXISTS $user;
CREATE DATABASE $fs_db ENGINE = Filesystem('$dir');
CREATE USER $user;
GRANT SELECT ON *.* TO $user;
EOF

# An uncached name is refused, ...
(( $(${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $fs_db.\`warm.csv\`" 2>&1 | grep -c "READ ON FILE") >= 1 )) && echo "READ ON FILE" || echo "UNEXPECTED";
# ... a user holding the grant caches the storage for that name, ...
${CLICKHOUSE_CLIENT} --query "SELECT * FROM $fs_db.\`warm.csv\`";
# ... and the cached name must be refused the same way.
(( $(${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $fs_db.\`warm.csv\`" 2>&1 | grep -c "READ ON FILE") >= 1 )) && echo "READ ON FILE" || echo "UNEXPECTED";
# The grant is checked before the filesystem is observed, so a name that does not exist reports the
# access error instead of telling the user whether the file is there.
(( $(${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $fs_db.\`missing.csv\`" 2>&1 | grep -c "READ ON FILE") >= 1 )) && echo "READ ON FILE" || echo "UNEXPECTED";

${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE TO $user";

# With the grant, both a cached name and a name that was never resolved before are served.
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $fs_db.\`warm.csv\`";
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $fs_db.\`cold.csv\`";

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE $fs_db;
DROP USER $user;
EOF
rm -rd "$dir"
