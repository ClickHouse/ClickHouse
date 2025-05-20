---
slug: /en/operations/settings/composable-protocols
sidebar_position: 64
sidebar_label: Composable Protocols
---

# Composable Protocols

Composable protocols allows more flexible configuration of TCP access to the ClickHouse server. This configuration can co-exist with or replace conventional configuration.

## Composable protocols section is denoted as `protocols` in configuration xml
**Example:**
``` xml
<protocols>

</protocols>
```

## Basic modules define protocol layers
**Example:**
``` xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```
where:
- `plain_http` - name which can be referred by another layer
- `type` - denotes protocol handler which will be instantiated to process data, set of protocol handlers is predefined:
  * `tcp` - native clickhouse protocol handler
  * `http` - http clickhouse protocol handler
  * `tls` - TLS encryption layer
  * `proxy1` - PROXYv1 layer
  * `mysql` - MySQL compatibility protocol handler
  * `postgres` - PostgreSQL compatibility protocol handler
  * `prometheus` - Prometheus protocol handler
  * `interserver` - clickhouse interserver handler

:::note
`gRPC` protocol handler is not implemented for `Composable protocols`
:::
 
## Endpoint (i.e. listening port) is denoted by `<port>` and (optional) `<host>` tags
**Example:**
``` xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```
If `<host>` is omitted, then `<listen_host>` from root config is used.

## Layers sequence is defined by `<impl>` tag, referencing another module
**Example:** definition for HTTPS protocol
``` xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

## Endpoint can be attached to any layer
**Example:** definition for HTTP (port 8123) and HTTPS (port 8443) endpoints
``` xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

## Additional endpoints can be defined by referencing any module and omitting `<type>` tag
**Example:** `another_http` endpoint is defined for `plain_http` module
``` xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

## Some modules can contain specific for its layer parameters
**Example:** for TLS layer private key (`privateKeyFile`) and certificate files (`certificateFile`) can be specified
``` xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```
