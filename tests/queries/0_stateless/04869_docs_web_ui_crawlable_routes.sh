#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every embedded reference entity is available at a stable HTTP path. The response contains its
# title, canonical URL, and Markdown body before JavaScript runs, so ordinary web crawlers can index
# it. A sitemap exposes every route, while `routes.json` provides the destination catalog used to
# build redirects from the previous documentation URLs.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs/functions/plus")"

echo "$PAGE" | grep -oF '<title>plus | ClickHouse Reference</title>' | head -n1
echo "$PAGE" | grep -oF "href=\"${URL}/docs/functions/plus\" data-doc-canonical" | head -n1
echo "$PAGE" | grep -oF '<div class="entity-type">Function</div>' | head -n1
echo "$PAGE" | grep -oF '<div class="entity-body server-rendered-markdown">' | head -n1
echo "$PAGE" | grep -oF '**Syntax**' | head -n1

${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${URL}/docs/functions/does-not-exist"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${URL}/docs/functions/plus/"

SITEMAP="$(${CLICKHOUSE_CURL} -sS "${URL}/docs/sitemap.xml")"
echo "$SITEMAP" | grep -oF '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' | head -n1
echo "$SITEMAP" | grep -oF "<loc>${URL}/docs/functions/plus</loc>" | head -n1

ROUTES="$(${CLICKHOUSE_CURL} -sS "${URL}/docs/routes.json")"
echo "$ROUTES" | jq -r '.[] | select(.name == "plus" and .type == "Function") | [.name, .type, .path] | @tsv'
