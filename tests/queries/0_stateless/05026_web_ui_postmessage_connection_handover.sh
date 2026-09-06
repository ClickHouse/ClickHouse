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
#
# The whole allowlist is pinned, not just one entry of it: dropping an origin silently breaks the
# consoles that embed these pages, and adding one silently widens who may hand over a connection.
# A bare host name is rejected in both spellings, so a regression to a scheme-less comparison
# (`h === 'clickhouse.com'`) is caught rather than passing because the exact origin is also present.
TRUSTED_ORIGINS="https://console.clickhouse.cloud https://console.clickhouse-staging.com https://clickhouse.com"

exact_origins()
{
    local content="$1"
    local origin

    for origin in ${TRUSTED_ORIGINS}
    do
        echo "${content}" | grep -qF "'${origin}'" || { echo loose; return; }
    done

    # The allowlist must contain nothing but those origins: count the entries of the array itself.
    local declared
    declared=$(echo "${content}" | sed -n "/TRUSTED_\(HOST\|PARENT\)_ORIGINS = \[/,/\];/p" | grep -c "^ *'")
    [ "${declared}" = "$(echo ${TRUSTED_ORIGINS} | wc -w)" ] || { echo loose; return; }

    ! echo "${content}" | grep -qE "endsWith\('\.clickhouse\.(cloud|com)'\)" \
        && ! echo "${content}" | grep -qF "'clickhouse.cloud'" \
        && ! echo "${content}" | grep -qF "'clickhouse.com'" \
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
    # A handover may omit `url`, and then the page keeps the endpoint it already has. That default is
    # the address the page was served from, path prefix included: a reverse proxy can expose the server
    # as `https://proxy.example/clickhouse/`, where `https://proxy.example/` is an unrelated site.
    # Seeding the connection from `location.origin` would aim every query at that unrelated root.
    default_endpoint=$(echo "$content" | grep -qF "function defaultServerAddress()" \
        && ! echo "$content" | grep -qE "= location.protocol != 'file:' \\? location.origin" \
        && echo prefixed || echo origin)
    # A handover replaces the credentials wholesale: when it carries none, the page clears the `user`
    # and `password` inputs, and reading them back must yield nothing. A page that falls back to the
    # previously configured value on an empty input (`... .value || user`) would keep authenticating to
    # the newly handed-over server with the credentials of the old one, which is the very leak the
    # handover exists to prevent.
    credentials=$(echo "$content" | grep -qE "\|\| (user|password)\b" && echo sticky || echo replaced)
    echo "${page} announces=${announces} accepts=${accepts} origins=${origins} endpoint_query=${endpoint_query} default_endpoint=${default_endpoint} credentials=${credentials}"
done

content=$(fetch_page webterminal)
handshake=$(echo "$content" | grep -qF "'webterminal-hello'" && echo yes || echo no)
accepts=$(echo "$content" | grep -qF "'webterminal-credentials'" && echo yes || echo no)
origins=$(exact_origins "$content")
# The WebSocket endpoint of the terminal is the very route this page was served from, so it must keep
# the path prefix of that route. A root-absolute `/webterminal` connects to whatever answers at the
# origin, which behind a path-routed reverse proxy is an unrelated site.
ws_route=$(echo "$content" | grep -qF "lastIndexOf('/webterminal')" \
    && ! echo "$content" | grep -qF "loc.host + '/webterminal'" \
    && echo prefixed || echo root)
# The query string of that route is part of the endpoint too: a proxy route can select a cluster or a
# namespace with `?cluster=a`, and `/play` opens the terminal at the address it is configured with.
# Dropping it here would authenticate the terminal against the proxy's default backend. The page's own
# `user` and `password` parameters are credentials rather than routing and must not be forwarded.
ws_query=$(echo "$content" | sed -n '/function getWebSocketURL()/,/^    }/p' \
    | grep -qF "query.delete('user')" \
    && echo "$content" | sed -n '/function getWebSocketURL()/,/^    }/p' | grep -qF "query.delete('password')" \
    && echo "$content" | sed -n '/function getWebSocketURL()/,/^    }/p' | grep -qF "loc.search" \
    && echo preserved || echo dropped)
echo "webterminal handshake=${handshake} accepts=${accepts} origins=${origins} ws_route=${ws_route} ws_query=${ws_query}"

content=$(fetch_page play)
relay=$(echo "$content" | grep -qF "clickhouse-docs-relay-ready" && echo yes || echo no)
direct_docs_opener=$(echo "$content" | grep -qF "window.open(docs_icon.href" && echo yes || echo no)
relay_trust_guard=$(echo "$content" | grep -qF "if (!isTrustedHostOrigin(origin)) return;" && echo yes || echo no)
relay_ordering_guard=$(echo "$content" | grep -qF "if (!credentials || !docs_frame_ready) return;" \
    && echo "$content" | grep -qF "docs_frame_ready = true;" \
    && echo yes || echo no)
echo "play docs_relay=${relay} direct_docs_opener=${direct_docs_opener} relay_trust_guard=${relay_trust_guard} relay_ordering_guard=${relay_ordering_guard}"

# The Web Terminal link, probe and iframe of `/play` address the configured server, and the path of
# that address is part of the endpoint, exactly like the Documentation link: a server exposed as
# `https://proxy.example/clickhouse/` serves its terminal under that prefix.
terminal_route=$(echo "$content" | grep -qF "+ 'webterminal';" \
    && ! echo "$content" | grep -qF "new URL('/webterminal'" \
    && echo prefixed || echo root)
# A handed-over connection may name a server other than the one that served this page, and a
# `PasswordCredential` is scoped to the page origin rather than to that server. Persisting a
# cross-origin login here would let the browser autofill it on a later plain visit, where the page
# defaults back to its own origin and would send that login to the wrong server.
# The origin alone is too coarse a gate: a path-routed reverse proxy serves unrelated backends under
# one origin, so a login handed over for `https://proxy.example/b/` must not be remembered by a page
# served from `https://proxy.example/a/play`. The whole endpoint identity is compared instead.
credential_store=$(echo "$content" | grep -qF "password_elem.value && isOwnServerTarget(url_elem.value)" \
    && echo "$content" | sed -n '/function isOwnServerTarget(value)/,/^}/p' \
        | grep -qF "sameServerAddress(value, defaultServerAddress())" \
    && echo endpoint || echo origin)
# The Documentation and Web Terminal URLs are written into a link `href`, an iframe `src` and the
# `docs_relay` parameter of the relay page. A `user:password@` userinfo of the configured address
# would therefore be put back into browser history, referrers and access logs.
derived_url_userinfo=$(test "$(echo "$content" | grep -cF 'stripURLCredentials(url);')" = 2 \
    && echo "$content" | sed -n '/function stripURLCredentials(url)/,/^}/p' | grep -qF "url.password = '';" \
    && echo stripped || echo kept)
# The query string of the configured address selects the endpoint (a proxy route can name a cluster or
# a namespace), so the Web Terminal has to be opened on that same endpoint. Only `user` and `password`
# are dropped - they are the terminal page's own parameters, and a password must not travel in a URL.
terminal_query=$(echo "$content" | sed -n '/function getTerminalURL()/,/^    }/p' \
    | grep -qF "url.searchParams.delete('user');" \
    && ! echo "$content" | sed -n '/function getTerminalURL()/,/^    }/p' | grep -qF "url.search = '';" \
    && echo preserved || echo dropped)
echo "play terminal_route=${terminal_route} terminal_query=${terminal_query} credential_store=${credential_store} derived_url_userinfo=${derived_url_userinfo}"

# `/schema` persists a credential under the same rule, and additionally retrieves a remembered one on
# open, which is the path that would fill a login saved for one endpoint into a page pointed at another.
content=$(fetch_page schema)
credential_store=$(echo "$content" | grep -qF "\$('password').value && isOwnServerTarget(\$('url').value)" \
    && echo "$content" | sed -n '/function isOwnServerTarget(value)/,/^}/p' | grep -qF "defaultServerAddress()" \
    && echo endpoint || echo origin)
credential_retrieval=$(echo "$content" | grep -qF "window.PasswordCredential && isOwnServerTarget(\$('url').value)" \
    && echo endpoint || echo origin)
echo "schema credential_store=${credential_store} credential_retrieval=${credential_retrieval}"
