#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A plan-gating badge such as `<ScalePlanFeatureBadge feature="S3 Role-Based Access" />` renders a
# substantive message from its attributes on the website (which plan the feature requires and how to
# get it, see `docs/snippets/components/ScalePlanFeatureBadge`). The built-in `/docs` page must
# render that message after the badge label (see `badgePayload` in `programs/server/docs.html`)
# instead of collapsing the whole tag to the badge name and losing the plan-gating warning.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `preprocessMarkdown` renders a plan-gating badge's message after the badge label ...
echo "$PAGE" | grep -oF 'function badgePayload(name, attributes) {' | head -n1
echo "$PAGE" | grep -oF "(payload ? ' ' + payload : '')" | head -n1
# ... building it from the badge's attributes the same way the website components do.
echo "$PAGE" | grep -oF 'available in the Scale and Enterprise plans. To upgrade, visit the plans page in the cloud console.' | head -n1
echo "$PAGE" | grep -oF 'Contact support to enable this feature.' | head -n1

# The regression target exists in the corpus: the `S3Queue` engine documentation opens with a
# prop-bearing `<ScalePlanFeatureBadge feature="..." />`. It requires S3 support, so it is absent
# from the minimal `Fast test` build (`ENABLE_LIBRARIES=0`); the check is therefore tolerant of its
# absence and asserts only that, when present, the badge still carries a `feature` attribute in the
# raw `system.documentation` content (so the regression input has not silently disappeared).
$CLICKHOUSE_CLIENT --query "
    SELECT count() = countIf(position(description, '<ScalePlanFeatureBadge feature=') > 0)
    FROM system.documentation
    WHERE type = 'Table Engine' AND name = 'S3Queue'"
