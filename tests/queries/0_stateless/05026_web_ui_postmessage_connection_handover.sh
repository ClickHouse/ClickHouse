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
#
# `/webterminal` predates the handover and speaks its own `webterminal-hello` /
# `webterminal-credentials` exchange with the page embedding it, but its list of trusted hosts must
# stay exact for the same reason, so it is checked here as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

fetch_page()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/$1"
}

# A trusted host can aim a page at a server of its own, so trust must be granted to whole origins
# named one by one: neither to a domain suffix, nor to a bare host name, which would ignore the
# scheme and accept a plaintext `http://` impostor.
exact_origins()
{
    echo "$1" | grep -qF "'https://console.clickhouse.cloud'" \
        && ! echo "$1" | grep -qE "endsWith\('\.clickhouse\.(cloud|com)'\)" \
        && ! echo "$1" | grep -qF "'clickhouse.cloud'" \
        && echo exact || echo loose
}

for page in play docs dashboard merges schema jemalloc processors-profile binary
do
    content=$(fetch_page "${page}")
    announces=$(echo "$content" | grep -qF "postMessage({type: 'clickhouse-hello'}" && echo yes || echo no)
    accepts=$(echo "$content" | grep -qF "'clickhouse-credentials'" && echo yes || echo no)
    origins=$(exact_origins "$content")
    # A handed-over server address may carry a query string - `?database=db`, or a proxy route
    # selecting a cluster or a namespace - and the page's own parameters have to be appended to it
    # with `&`. Starting them with a second `?` would fold the first one into the value of the last
    # existing parameter and send the query to a different endpoint than the one handed over.
    endpoint_query=$(echo "$content" | grep -qF "indexOf('?') >= 0" && echo preserved || echo dropped)
    echo "${page} announces=${announces} accepts=${accepts} origins=${origins} endpoint_query=${endpoint_query}"
done

content=$(fetch_page webterminal)
handshake=$(echo "$content" | grep -qF "'webterminal-hello'" && echo yes || echo no)
accepts=$(echo "$content" | grep -qF "'webterminal-credentials'" && echo yes || echo no)
origins=$(exact_origins "$content")
echo "webterminal handshake=${handshake} accepts=${accepts} origins=${origins}"

content=$(fetch_page play)
relay=$(echo "$content" | grep -qF "clickhouse-docs-relay-ready" && echo yes || echo no)
direct_docs_opener=$(echo "$content" | grep -qF "window.open(docs_icon.href" && echo yes || echo no)
relay_trust_guard=$(echo "$content" | grep -qF "if (!isTrustedHostOrigin(origin)) return;" && echo yes || echo no)
relay_ordering_guard=$(echo "$content" | grep -qF "if (!credentials || !docs_frame_ready) return;" \
    && echo "$content" | grep -qF "docs_frame_ready = true;" \
    && echo yes || echo no)
echo "play docs_relay=${relay} direct_docs_opener=${direct_docs_opener} relay_trust_guard=${relay_trust_guard} relay_ordering_guard=${relay_ordering_guard}"
