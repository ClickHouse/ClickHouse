#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` documentation search page marks an entity that is only an alias of another
# with a small "alias" badge next to its name, so aliases are recognizable in the result list
# without opening them. An alias is recognized by its generated description, "Alias of `name`.",
# which `system.documentation` stores for every alias. This guards that the served page carries the
# badge styling and the detection/rendering logic, and that there are aliases for it to mark.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# The badge has its own styling ...
echo "$PAGE" | grep -oF '.alias-badge {' | head -n1

# ... is detected from the "Alias of `name`." description ...
echo "$PAGE" | grep -oF 'function aliasTarget' | head -n1

# ... and is rendered into the result list with the canonical name in its tooltip.
echo "$PAGE" | grep -oF "badge.className = 'alias-badge'" | head -n1
echo "$PAGE" | grep -oF "badge.title = 'Alias of '" | head -n1

# There are aliases in `system.documentation` for the badge to mark.
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.documentation WHERE description LIKE 'Alias of %'"
