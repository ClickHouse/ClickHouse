#!/usr/bin/env bash

# Every built-in web UI page implements the `postMessage` connection handover: it announces itself
# to the window that opened or embedded it with `clickhouse-hello`, and adopts the server URL, user
# and password sent back in a `clickhouse-credentials` message. That is how `/play` hands its
# connection to `/docs`, and it is what lets a password reach these pages without travelling in a
# URL, where it would be recorded in browser history, referrer headers and access logs.
#
# The set of hosts a page accepts a connection from must stay a list of exact origins. A domain
# suffix such as `*.clickhouse.cloud` would trust every ClickHouse Cloud service endpoint, and a
# service endpoint serves whatever content its owner chooses.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for page in play docs dashboard merges schema jemalloc processors-profile binary
do
    content=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/${page}")
    announces=$(echo "$content" | grep -qF "postMessage({type: 'clickhouse-hello'}" && echo yes || echo no)
    accepts=$(echo "$content" | grep -qF "'clickhouse-credentials'" && echo yes || echo no)
    origins=$(echo "$content" | grep -qF "'https://console.clickhouse.cloud'" \
        && ! echo "$content" | grep -qF "endsWith('.clickhouse.cloud')" && echo exact || echo loose)
    echo "${page} announces=${announces} accepts=${accepts} origins=${origins}"
done

content=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play")
relay=$(echo "$content" | grep -qF "clickhouse-docs-relay-ready" && echo yes || echo no)
direct_docs_opener=$(echo "$content" | grep -qF "window.open(docs_icon.href" && echo yes || echo no)
relay_trust_guard=$(echo "$content" | grep -qF "if (!isTrustedHostOrigin(origin)) return;" && echo yes || echo no)
relay_ordering_guard=$(echo "$content" | grep -qF "if (!credentials || !docs_frame_ready) return;" \
    && echo "$content" | grep -qF "docs_frame_ready = true;" \
    && echo yes || echo no)
echo "play docs_relay=${relay} direct_docs_opener=${direct_docs_opener} relay_trust_guard=${relay_trust_guard} relay_ordering_guard=${relay_ordering_guard}"
