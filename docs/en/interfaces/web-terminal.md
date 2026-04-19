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
The web terminal is available on Linux builds only. It is an experimental feature and is disabled by default; see [Enabling the feature](#enabling-the-feature) below.
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

The web terminal authenticates with the same mechanisms as the HTTP protocol:

- URL query parameters (`user`, `password`).
- HTTP Basic authentication.
- `X-ClickHouse-User` and `X-ClickHouse-Key` headers.

Credentials are sent in the first WebSocket message rather than as part of the upgrade URL, so they are not exposed in access logs or browser history. Invalid credentials cause the server to close the WebSocket with code `1008`; the browser UI re-prompts for credentials.

## What the session looks like {#session}

Once authenticated, the server runs `clickhouse-client` attached to a pseudoterminal and bridges its input and output over WebSocket. The session supports the full `clickhouse-client` experience, including:

- Syntax highlighting.
- Autocompletion.
- Multi-line queries.
- Command history (stored on the server side for the duration of the session).

The terminal uses [xterm.js](https://xtermjs.org/) for rendering. All assets are served from the ClickHouse binary itself — no third-party CDNs are loaded.

## Integration with `/play` {#play-integration}

The [`/play`](/interfaces/http) Web SQL UI embeds the web terminal as a dockable panel. Toggle it with the terminal icon in the sidebar or press the `~` key when the query editor is empty. The `/play` page detects `/webterminal` availability at load time and hides the terminal controls when the endpoint is unavailable (disabled, or non-Linux builds).

## Security considerations {#security}

The web terminal exposes an interactive shell-like session to anyone who can authenticate against the ClickHouse HTTP endpoint, so the same caveats that apply to the HTTP protocol apply here:

- Always serve `/webterminal` over HTTPS in untrusted environments to protect credentials and session traffic.
- Restrict access at the network level (firewall, reverse proxy, or the `listen_host` configuration) the same way you restrict access to the HTTP protocol.
- The endpoint validates the `Origin` header against the `Host` to mitigate cross-origin WebSocket hijacking; configure reverse proxies accordingly if you terminate TLS externally.

The handler also enforces WebSocket protocol conformance per RFC 6455: unmasked client frames, reserved opcodes, oversized or fragmented control frames, and reserved RSV bits are rejected with protocol-error close codes.

## Platform availability {#platform}

The handler is compiled only on Linux builds. On other platforms, `/webterminal` is not registered and the links to it on the ClickHouse start page and in `/play` are hidden automatically.
