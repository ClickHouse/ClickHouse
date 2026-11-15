#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` documentation search page renders Markdown coming from `system.documentation`
# on the server the user points it at, in the same origin that holds the connection credentials.
# `marked.parse` is not a sanitizer, so its output must never reach the DOM directly: untrusted
# documentation could otherwise inject `<img src=x onerror=...>` or `[x](javascript:...)` that runs
# in this origin. This guards that the served page routes rendered Markdown through the sanitizer,
# keeps the element/attribute allowlist and the URL-scheme policy, and no longer has the vulnerable
# direct-`innerHTML` sink.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# Rendered documentation is inserted through the sanitizer, never as raw `innerHTML`.
echo "$PAGE" | grep -oF 'setSanitizedHTML(body, html)' | head -n1

# The vulnerable pattern (the `marked.parse` result assigned straight to `innerHTML`) is gone.
echo "$PAGE" | grep -cF 'body.innerHTML = html' ||:

# The sanitizer enforces an element and attribute allowlist ...
echo "$PAGE" | grep -oF 'const ALLOWED_TAGS' | head -n1
echo "$PAGE" | grep -oF 'const ALLOWED_ATTRS' | head -n1

# ... and rejects unsafe URL schemes: only http/https/mailto (and relative/anchor links) pass, so
# `javascript:`, `data:`, `vbscript:`, ... are dropped from `href`/`src`.
echo "$PAGE" | grep -oF 'function isUnsafeURL' | head -n1
echo "$PAGE" | grep -oF "scheme !== 'http' && scheme !== 'https' && scheme !== 'mailto'" | head -n1

# ... and rejects resource sinks that auto-load against this credentialed origin when the document
# renders: a same-origin `src` (e.g. `![x](/?query=SELECT%201)`) and a `url(...)` inside a `style`.
echo "$PAGE" | grep -oF 'function isSameOriginURL' | head -n1
echo "$PAGE" | grep -oF "name === 'src' && (isUnsafeURL(attr.value) || isSameOriginURL(attr.value))" | head -n1
echo "$PAGE" | grep -oF 'function sanitizeStyle' | head -n1
