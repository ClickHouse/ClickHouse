---
description: 'Documentation for the web terminal, an in-browser `clickhouse-client` session over WebSocket'
sidebar_label: 'Web Terminal'
sidebar_position: 22
slug: /interfaces/web-terminal
title: 'Web Terminal'
doc_type: 'reference'
---

The web terminal is an experimental in-browser interface that provides an interactive `clickhouse-client` session over WebSocket. It is served from any ClickHouse HTTP port at the `/webterminal` path.

:::note
The web terminal is an experimental feature and is disabled by default; see [Enabling the feature](#enabling-the-feature) below.
:::

## Enabling the feature {#enabling-the-feature}

The `/webterminal` endpoint is gated by the `allow_experimental_webterminal` server setting. When the setting is `false` (the default), requests to `/webterminal` return HTTP status `403 Forbidden`.

To enable it, add the following to your server configuration:

```xml
<clickhouse>
    <allow_experimental_webterminal>true</allow_experimental_webterminal>
</clickhouse>
```

After enabling, navigate to `/webterminal` on any ClickHouse HTTP port (for example, `http://localhost:8123/webterminal`) to open the terminal.

## Authentication {#authentication}

The web terminal authenticates the user against the same `Session` and access-control checks as the HTTP protocol, but credentials are exchanged in-band over the established WebSocket connection rather than via the HTTP upgrade request. After the WebSocket handshake completes, the browser sends the first message as JSON:

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

This avoids placing credentials in URL query parameters or `Authorization` headers attached to the upgrade request, where they could end up in browser history, server access logs, and reverse-proxy logs. URL parameters, HTTP Basic, and `X-ClickHouse-User`/`X-ClickHouse-Key` headers on the upgrade request are intentionally **not** consulted by `/webterminal`.

Invalid credentials cause the server to close the WebSocket with code `1008`; the browser UI re-prompts for credentials.

## What the session looks like {#session}

Once authenticated, the server runs `clickhouse-client` attached to a pseudoterminal and bridges its input and output over WebSocket. The session supports the full `clickhouse-client` experience, including:

- Syntax highlighting.
- Autocompletion.
- Multi-line queries.
- Command history (stored on the server side for the duration of the session).

The terminal uses [xterm.js](https://xtermjs.org/) for rendering. All assets are served from the ClickHouse binary itself — no third-party CDNs are loaded.

## Integration with `/play` {#play-integration}

The [`/play`](/interfaces/http) Web SQL UI embeds the web terminal as a dockable panel. Toggle it with the terminal icon in the sidebar or press the `~` key when the query editor is empty. The `/play` page detects `/webterminal` availability at load time and hides the terminal controls when the endpoint is unavailable (for example, when the experimental setting is not enabled).

## Security considerations {#security}

The web terminal exposes an interactive shell-like session to anyone who can authenticate against the ClickHouse HTTP endpoint, so the same caveats that apply to the HTTP protocol apply here:

- Always serve `/webterminal` over HTTPS in untrusted environments to protect credentials and session traffic.
- Restrict access at the network level (firewall, reverse proxy, or the `listen_host` configuration) the same way you restrict access to the HTTP protocol.
- The endpoint validates the `Origin` header against the `Host` to mitigate cross-origin WebSocket hijacking; configure reverse proxies accordingly if you terminate TLS externally.
- Behind a TLS-terminating reverse proxy, the upstream connection to ClickHouse is plain `http` even though the browser uses `https`, so the strict same-origin check would reject legitimate connections. For these deployments, set `webterminal_allowed_origins` to a comma-separated list of full origins that are allowed to open WebSocket sessions; when this setting is non-empty, it replaces the default same-origin check. Example: `<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`.

The handler also enforces WebSocket protocol conformance per RFC 6455: unmasked client frames, reserved opcodes, oversized or fragmented control frames, and reserved RSV bits are rejected with protocol-error close codes.

## Platform availability {#platform}

The handler is compiled on all platforms ClickHouse supports. The pseudoterminal layer used by the embedded `clickhouse-client` runner is implemented on top of portable POSIX primitives (`posix_openpt`/`grantpt`/`unlockpt`), with a Linux-specific path that uses the thread-safe `ptsname_r`. The links to `/webterminal` on the ClickHouse start page and in `/play` are hidden automatically when the endpoint is unavailable (for example, when `allow_experimental_webterminal` is not enabled).
