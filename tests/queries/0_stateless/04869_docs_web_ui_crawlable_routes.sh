#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every embedded reference entity is available at a stable HTTP path. The response contains its
# title, root-relative canonical URL, and Markdown body before JavaScript runs, so ordinary web
# crawlers can index it without trusting the request's `Host` header. A sitemap is enabled with the
# `documentation_public_url` server setting, while `routes.json` provides the destination catalog
# used to build redirects from the previous documentation URLs.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs/functions/plus")"

echo "$PAGE" | grep -oF '<title>plus | ClickHouse Reference</title>' | head -n1
echo "$PAGE" | grep -oF 'href="/docs/functions/plus" data-doc-canonical' | head -n1
echo "$PAGE" | grep -oF '<div class="entity-type">Function</div>' | head -n1
echo "$PAGE" | grep -oF '<div class="entity-body server-rendered-markdown">' | head -n1
echo "$PAGE" | grep -oF '**Syntax**' | head -n1

${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${URL}/docs/functions/does-not-exist"
${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${URL}/docs/functions/plus/"
${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${URL}/docs/functions/plus/?user=play" | tr -d '\r' | grep -oF 'Location: /docs/functions/plus?user=play' | head -n1

${CLICKHOUSE_CURL} -sS -o /dev/null -w '%{http_code}\n' "${URL}/docs/sitemap.xml"

${CLICKHOUSE_CURL} -sS "${URL}/docs/assets/logo-light.svg" | grep -oF '<svg width="172" height="33"' | head -n1

ROUTES="$(${CLICKHOUSE_CURL} -sS "${URL}/docs/routes.json")"
echo "$ROUTES" | jq -r '.[] | select(.name == "plus" and .type == "Function") | [.name, .type, .path] | @tsv'
