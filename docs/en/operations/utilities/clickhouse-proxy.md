---
description: 'Guide to clickhouse-proxy, a native load-balancing and routing proxy for ClickHouse'
sidebar_label: 'clickhouse-proxy'
sidebar_position: 62
slug: /operations/utilities/clickhouse-proxy
title: 'clickhouse-proxy'
doc_type: 'reference'
---

# clickhouse-proxy {#clickhouse-proxy}

`clickhouse-proxy` is a native proxy server that can be deployed in front of one
or more ClickHouse servers. It accepts client connections over the native
protocol and forwards them to an upstream server selected according to
configurable routing rules and load-balancing policies.

It is started as a standalone program:

```bash
clickhouse proxy --config-file /etc/clickhouse-proxy/config.xml
```

When run without a configuration file, the proxy uses a built-in configuration
that listens on `tcp_port` `9090` and forwards every connection to a single
upstream on `127.0.0.1:9011`.

## Configuration {#configuration}

The proxy is configured through an XML (or YAML) file, in the same style as the
ClickHouse server configuration. The top-level `tcp_port` is the port the proxy
listens on; `tcp_with_proxy_port`, if set, is a separate port on which the proxy
expects an incoming PROXY-protocol header.

### Upstreams {#upstreams}

Upstream servers are declared as named clusters of replicas under
`<upstreams><clusters>`. Each cluster is a named group of `<replica>` entries,
and each replica is identified by its `host` and `tcp_port`:

```xml
<upstreams>
    <clusters>
        <cluster_a>
            <replica>
                <host>127.0.0.1</host>
                <tcp_port>9011</tcp_port>
            </replica>
        </cluster_a>
        <cluster_b>
            <replica>
                <host>127.0.0.1</host>
                <tcp_port>9011</tcp_port>
            </replica>
            <replica>
                <host>127.0.0.1</host>
                <tcp_port>9012</tcp_port>
            </replica>
        </cluster_b>
    </clusters>
</upstreams>
```

### Routing {#routing}

Routing rules are evaluated in order under `<routing><rules>`. A rule matches a
connection when all of the conditions it declares (`user`, `database`, `host`)
are satisfied; a condition that is omitted matches any value. The first matching
rule is applied. If no rule matches, the `<routing><default>` rule is used; if
there is no default rule, the connection is rejected.

A rule's `<action>` either rejects the connection or routes it to a cluster:

```xml
<routing>
    <rules>
        <rule>
            <user>admin</user>
            <action>
                <route_to>cluster_b</route_to>
            </action>
        </rule>
        <rule>
            <database>deprecated_database</database>
            <action>
                <reject>true</reject>
            </action>
        </rule>
    </rules>
    <default>
        <action>
            <reject>true</reject>
        </action>
    </default>
</routing>
```

### Load-balancing policies {#load-balancing-policies}

When a rule routes to a cluster with more than one replica, the replica is
chosen according to the rule's `<policy>`. The supported policies are:

- `round_robin` (the default) — distributes connections sequentially across the
  replicas of the cluster.
- `least_connections` — prefers the replica with the fewest active connections.

```xml
<rule>
    <user>test_user</user>
    <action>
        <route_to>cluster_b</route_to>
        <policy>least_connections</policy>
    </action>
</rule>
```
