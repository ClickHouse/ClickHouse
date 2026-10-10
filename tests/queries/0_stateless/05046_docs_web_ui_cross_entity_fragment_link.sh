#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The embedded documentation refers to a sibling entity with a bare fragment link, e.g. the
# `enable_group_by_top_k_optimization` setting links to
# `[query_plan_max_limit_for_top_k_optimization](#query_plan_max_limit_for_top_k_optimization)`.
# That works on the documentation website, where all the settings share a single page, but in the
# built-in `/docs` page every entity has a page of its own, so the fragment has no target in the
# document and clicking such a link did nothing. `rewriteLinks` in `programs/server/docs.html` must
# resolve a fragment without an in-page target to the entity it names and open that entity instead.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# A fragment link is an in-page anchor only when the rendered document really contains that id ...
echo "$PAGE" | grep -oF 'const entity = body.querySelector(`[id="${CSS.escape(id)}"]`)' | head -n1
# ... otherwise it is resolved to the entity the fragment names, and opened in the app.
echo "$PAGE" | grep -oF 'resolveDocEntity(id, href) || resolveDocEntity(candidateTerm(a, href), href)' | head -n1
# The "#" anchors of the headings are excluded from that handling: their href is a whole app state
# hash, which would otherwise be taken for an element id and pushed into the URL as a section.
echo "$PAGE" | grep -oF "if (a.classList.contains('heading-anchor')) continue;" | head -n1

# The regression input exists in the corpus: the link above, and its target, are both documented.
$CLICKHOUSE_CLIENT --query "
    SELECT count() = 1
    FROM system.documentation
    WHERE type = 'Setting' AND name = 'enable_group_by_top_k_optimization'
      AND position(description, '](#query_plan_max_limit_for_top_k_optimization)') > 0"

$CLICKHOUSE_CLIENT --query "
    SELECT count() = 1
    FROM system.documentation
    WHERE type = 'Setting' AND name = 'query_plan_max_limit_for_top_k_optimization'"

# Such cross-entity fragment links are pervasive, not a single case: many entities carry a fragment
# link whose target is the name of another documented entity.
$CLICKHOUSE_CLIENT --query "
    WITH arrayMap(x -> splitByChar(')', x)[1], arrayPopFront(splitByString('](#', description))) AS fragments
    SELECT count() > 100
    FROM system.documentation
    WHERE arrayExists(f -> f IN (SELECT name FROM system.documentation), fragments)"
