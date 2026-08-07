---
description: 'The clickhouse-proxy application mode — a lightweight protocol-aware proxy that routes end-user ClickHouse connections to backend servers.'
sidebar_label: 'clickhouse-proxy'
sidebar_position: 70
slug: /operations/clickhouse-proxy
title: 'ClickHouse proxy'
doc_type: 'reference'
---

# ClickHouse proxy {#clickhouse-proxy}

`clickhouse proxy` is an application mode of the `clickhouse` binary that accepts connections over
end-user ClickHouse protocols, chooses a backend server from a configurable routing table, and
forwards the traffic to it. It is designed to sustain many concurrent connections with low memory
usage: every connection is handled by a lightweight fiber on a cooperative scheduler (the `silk`
fiber framework) instead of an operating-system thread.

Start it with:

```bash
clickhouse proxy --config-file proxy_config.xml
```

:::note
The proxy is built on the `silk` fiber framework, which requires Linux with `io_uring` on `x86-64`
(v2 or newer) or `AArch64`. On other platforms `clickhouse proxy` reports that it is disabled.
:::

## Supported protocols {#supported-protocols}

The proxy handles the protocols an end user speaks to ClickHouse. It does not handle the Keeper
protocol or the inter-server replication protocol.

| Protocol      | Listener `protocol` | Routing by user / database                          |
|---------------|---------------------|-----------------------------------------------------|
| HTTP(S)       | `http`              | Yes (headers, URL parameters, Basic authentication) |
| Native TCP    | `native`            | Yes (parsed from the `Hello` packet)                |
| PostgreSQL    | `postgresql`        | Yes (parsed from the `StartupMessage`)              |
| MySQL         | `mysql`             | By user or database (terminated; see below)         |
| SSH           | `ssh`               | By the client's public key (terminated; see below)  |
| TLS by SNI    | `tls`               | By hostname (SNI), without decryption               |
| Opaque stream | `stream`            | By peer address or default pool                     |

Query-type routing (see below) applies to the HTTP protocol only.

## MySQL {#mysql}

MySQL is server-speaks-first and negotiates TLS in-band, so the proxy cannot read the user name or
the database from a passive first packet. When a routing rule needs the user or the database the
proxy therefore terminates the handshake: it greets the client, reads the `HandshakeResponse`
(user, database), and routes on them. Because the client's authentication response is bound to the
scramble in the greeting, the proxy connects to the chosen backend, reads the backend's scramble,
and issues an auth-switch so the client re-computes its response against the backend's scramble; the
proxy forwards that verbatim. The backend performs the real password check — the proxy never learns
the password. After authentication the connection is spliced byte for byte.

When no routing rule needs the user or the database, MySQL connections are instead forwarded
transparently and routed by peer address or the listener's default pool (no termination). Only
`mysql_native_password` is mediated; TLS is not offered to the client on a MySQL listener so that the
handshake stays readable.

## SSH {#ssh}

SSH carries no cleartext identity (there is no equivalent of TLS SNI, and the user name is only sent
inside the encrypted transport, after key exchange). To route it, the proxy therefore *terminates*
SSH: it presents its own host key, completes the handshake, and reads the client's offered public
key during authentication. It routes by that key — a pool is chosen by which key the client presents
— and then re-originates a fresh SSH connection to the selected backend as a bastion, splicing the
two sessions (pty, shell, exec, and their input/output).

Because the client's signature cannot be replayed to the backend, the proxy authorizes the client by
its public key and logs in to the backend with a configured bastion key that the backend trusts:

```xml
<proxy>
    <listeners>
        <listener><protocol>ssh</protocol><port>22</port></listener>
    </listeners>
    <ssh>
        <host_key_file>/etc/clickhouse-proxy/ssh_host_ed25519_key</host_key_file>
        <backend_user>default</backend_user>
        <backend_key_file>/etc/clickhouse-proxy/bastion_ed25519_key</backend_key_file>
    </ssh>
    <pools>
        <pool>
            <name>tenant-a</name>
            <backend><host>tenant-a.example.net</host><ssh_port>9022</ssh_port></backend>
        </pool>
    </pools>
    <rules>
        <rule>
            <!-- Route by the client's public key. Use the whole "authorized_keys" file or list keys inline. -->
            <authorized_key_file>/etc/clickhouse-proxy/tenant-a.keys</authorized_key_file>
            <pool>tenant-a</pool>
        </rule>
        <rule>
            <authorized_key>ssh-ed25519 AAAAC3Nza...tenant-b-key</authorized_key>
            <pool>tenant-b</pool>
        </rule>
    </rules>
</proxy>
```

Each SSH connection borrows an OS thread for its lifetime (libssh drives its own blocking I/O), so
SSH does not scale as cheaply as the byte-spliced protocols; it is intended for interactive and
command sessions rather than very high connection counts.

Entries of an allowlist follow the `authorized_keys` syntax, `[options] <type> <base64> [comment]`;
the options field, the comment, and `#` comment lines are ignored. A rule that specifies an
allowlist from which no key can be parsed is rejected at startup, so a malformed allowlist can never
degrade into a rule that accepts every key.

An `ssh` listener requires both `host_key_file` and `backend_key_file`, and both key files are
loaded once at startup: a missing, unreadable, or unparseable key — or a build without SSH support —
fails the start instead of silently dropping every connection at runtime.

## TLS {#tls}

A listener can handle TLS in three ways:

- **Terminate and re-encrypt** — set `<secure>1</secure>` on the listener. The proxy holds its own
  certificate (from the `openSSL.server` section), decrypts the connection, and — if the chosen
  backend is marked `<secure>1</secure>` — opens an independent TLS connection to it.
- **Terminate only (unwrap)** — a secure listener with plaintext backends. The proxy decrypts and
  speaks to the backends without encryption.
- **Transparent** — the `tls` listener protocol. The proxy reads the SNI from the `ClientHello`,
  chooses a backend by hostname, and forwards the encrypted bytes without decrypting them.

Certificates can be provisioned automatically with ACME (for example Let's Encrypt) by adding an
`acme` section, exactly as for `clickhouse-server`. The ACME HTTP-01 challenge is served on the
HTTP listeners at `/.well-known/acme-challenge/`.

## Configuration {#configuration}

The whole configuration lives under a top-level `proxy` element. See
`programs/proxy/proxy_config.xml` for a complete, commented example.

### Listeners {#listeners}

Each `listener` binds a port and selects a protocol. The optional `<pool>` names the pool to use
when no routing rule matches. The optional `<peek>` (`auto`, `none`, `credentials`, `query`)
controls how deeply the proxy inspects the beginning of a connection; `auto` (the default) inspects
only as much as the routing rules require.

```xml
<listener>
    <protocol>http</protocol>
    <port>8123</port>
    <pool>analytics</pool>
</listener>
```

### Pools and backends {#pools-and-backends}

A pool is a named group of backends with a load-balancing strategy. Listing several backends in one
pool provides load balancing; a backend may specify per-protocol ports and a `<weight>`.

```xml
<pool>
    <name>analytics</name>
    <load_balancing>least_connections</load_balancing>
    <backend>
        <host>backend-1.example.net</host>
        <tcp_port>9000</tcp_port>
        <http_port>8123</http_port>
        <weight>2</weight>
    </backend>
    <backend>
        <host>backend-2.example.net</host>
    </backend>
</pool>
```

The available load-balancing strategies are `random`, `round_robin`, `least_connections`,
`lowest_latency` and `least_resources`. Strategies share a common interface and can be extended.

### Routing rules {#routing-rules}

Rules are checked in order; the first whose criteria all match wins. A rule can match on `host`
(the TLS SNI or the HTTP `Host` header), `user`, `database`, `query_type` (`select`, `insert` or
`other`; HTTP only), and `protocol`. It routes either to a named `pool` or to a `backend_template`.
Hostnames are matched case-insensitively, as DNS names are case-insensitive: the incoming value is
lowercased before matching, exact `host` values are compared after lowercasing, and a `host_regexp`
is compiled as case-insensitive (regexp captures substituted into a `backend_template` are therefore
lowercase). `user` and `database` are matched case-sensitively.

Values can be matched exactly (or as a comma-separated list) or by a regular expression. A regular
expression may contain capture groups whose values are substituted for `$1` … `$9` in a
`backend_template`, so that, for example, users named `ch-<tenant>` are routed to per-tenant
backends:

```xml
<rule>
    <user_regexp>ch-(\w+)</user_regexp>
    <backend_template>
        <host>$1.backends.example.net</host>
        <tcp_port>9000</tcp_port>
    </backend_template>
</rule>
```

The routing table is abstract: it exposes hooks that run a shell command when the host, user or
database is unknown, when no backend of a pool is available, or the first time a user name or a
database name is seen. A hook can, for example, provision a backend and wait for it to become
available. Each hook is invoked as `<command> KIND PROTOCOL HOST USER DATABASE`.

### Session stickiness {#session-stickiness}

Repeated connections can be pinned to the same backend, chosen by a consistent
(rendezvous) hash so the choice is stable as backends are added or removed:

```xml
<stickiness>
    <by_session_id>1</by_session_id>    <!-- session_id from the HTTP URL -->
    <by_peer_address>0</by_peer_address>
</stickiness>
```

### Health monitoring {#health-monitoring}

The proxy periodically probes every backend with a TCP connection, records latency, and marks a
backend down after a number of consecutive failures (bringing it back up when it recovers). If a
backend has monitoring credentials (`<monitor_user>` / `<monitor_password>`), the proxy also polls
its CPU and memory usage over HTTP, which the `least_resources` strategy uses.

### HTTP extras {#http-extras}

An HTTP listener can answer `/ping`, serve static pages, and expose a JSON status page describing
all pools, their backends, and per-backend statistics. These endpoints are served by the proxy
itself and do not require a user name or a backend.

```xml
<http>
    <ping_path>/ping</ping_path>
    <status_path>/proxy_status</status_path>
    <static>
        <page>
            <path>/</path>
            <content><![CDATA[<html><body>ClickHouse proxy</body></html>]]></content>
        </page>
    </static>
</http>
```

### Performance and tuning {#performance-and-tuning}

For small request/response round-trips the proxy adds one extra network hop and a fiber hand-off.
Because every connection is a fiber rather than a thread, memory scales gently with connection
count: an idle connection costs its relay buffers and the pages its fiber stack has touched, not a
whole thread stack.

Plaintext connections (both legs unencrypted) are relayed with `splice(2)`, moving bytes through a
kernel pipe without copying them into user space; a TLS-terminated leg falls back to a user-space
copy because its bytes must be decrypted and re-encrypted.

Two settings trade throughput against memory:

- `relay_buffer_size` (default 256 KiB) is the per-direction relay chunk (the pipe size for a splice
  relay). Bulk transfers are buffer-bound: a small buffer limits the throughput of a single stream,
  and raising it lifts that limit until the network, rather than the relay, is the bottleneck. Each
  actively-transferring connection holds about twice this much memory, so lower it for very many
  mostly-idle connections and raise it for throughput-heavy workloads.
- `fiber_stack_size` (default 512 KiB) is the per-fiber stack size. It must stay large enough for
  the TLS handshake (which runs on a fiber); stacks are allocated lazily, so only the touched pages
  count against resident memory.

The achievable numbers depend on the network, on whether a leg is TLS-terminated, and on the request
mix, so measure them on your own hardware rather than assuming a particular rate.
