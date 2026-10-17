#!/usr/bin/env bash

# `timeSeriesTagsToGroup` and `timeSeriesGroupToTags` can't pass data out of the body of a view with `SQL SECURITY DEFINER`:
# the tags collector of the query isn't shared with the body, so a user reading the view can't decode the groups registered inside it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    CREATE TABLE private (secret String) ENGINE = MergeTree ORDER BY secret;
    INSERT INTO private VALUES ('hidden');
    CREATE VIEW v SQL SECURITY DEFINER AS SELECT sum(timeSeriesTagsToGroup([('secret', secret)])) AS total FROM private;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v TO ${user};
"

# The view returns the number of the group registered in its body, which is then used to look its tags up outside the view.
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT total FROM v"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT timeSeriesGroupToTags(total) FROM v" 2>&1 | grep -E "^Code: " | grep -oE "\(BAD_ARGUMENTS\)"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE v;
    DROP TABLE private;
    DROP USER ${user};
"
