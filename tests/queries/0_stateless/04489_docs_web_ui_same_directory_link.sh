#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page rewrites a relative documentation link that does not point to a documented
# entity into a `https://clickhouse.com/docs` URL. A bare same-directory link such as `textindexes.md`
# carries no section information, and the entity's embedded `source` is a code path (not a docs path), so
# its docs route cannot be determined. `toDocsURL` must therefore fall back to the docs root rather than
# fabricate a `/docs/textindexes` path that 404s. The embedded documentation itself avoids the ambiguity
# by spelling such cross-section references as absolute docs routes
# (e.g. `/engines/table-engines/mergetree-family/textindexes`).

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `toDocsURL` falls back to the docs root for a relative link whose route it cannot determine.
echo "$PAGE" | grep -oF 'rather than emit a fabricated path that 404s, fall back to the docs root.' | head -n1

# The MergeTree table engine is core, so it is present even in the minimal `Fast test` build
# (`ENABLE_LIBRARIES=0`). Its embedded documentation references the text-index and ANN-index pages with
# absolute docs routes, not bare same-directory `*.md` links that the `/docs` page cannot resolve.
$CLICKHOUSE_CLIENT --query "
    SELECT
        position(description, '/engines/table-engines/mergetree-family/textindexes') > 0
            AND position(description, '/engines/table-engines/mergetree-family/annindexes') > 0
            AND position(description, '](textindexes.md)') = 0
            AND position(description, '](annindexes.md)') = 0
    FROM system.documentation
    WHERE type = 'Table Engine' AND name = 'MergeTree'"
