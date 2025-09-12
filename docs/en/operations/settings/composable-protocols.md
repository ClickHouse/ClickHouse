---
description: 'Composable protocols allows more flexible configuration of TCP access
  to the ClickHouse server.'
sidebar_label: 'Composable Protocols'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Composable Protocols'
---

# Composable Protocols

## Overview {#overview}

Composable protocols allow more flexible configuration of TCP access to the 
ClickHouse server. This configuration can co-exist alongside, or replace, 
conventional configuration.

## Configuring composable protocols {#composable-protocols-section-is-denoted-as-protocols-in-configuration-xml}

Composable protocols can be configured in an XML configuration file. The protocols
section is denoted with `protocols` tags in the XML config file: 

```xml
<protocols>

</protocols>
```

### Configuring protocol layers {#basic-modules-define-protocol-layers}

You can define protocol layers using basic modules. For example, to define an
HTTP layer, you can add a new basic module to the `protocols` section:

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```
Modules can be configured according to:

- `plain_http` - name which can be referred to by another layer
- `type` - denotes the protocol handler which will be instantiated to process data.
   It has the following set of predefined protocol handlers:
  * `tcp` - native clickhouse protocol handler
  * `http` - HTTP clickhouse protocol handler
  * `tls` - TLS encryption layer
  * `proxy1` - PROXYv1 layer
  * `mysql` - MySQL compatibility protocol handler
  * `postgres` - PostgreSQL compatibility protocol handler
  * `prometheus` - Prometheus protocol handler
  * `interserver` - clickhouse interserver handler

:::note
`gRPC` protocol handler is not implemented for `Composable protocols`
:::
 
### Configuring endpoints {#endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags}

Endpoints (listening ports) are denoted by `<port>` and optional `<host>` tags.
For example, to configure an endpoint on the previously added HTTP layer we 
could modify our configuration as follows:

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

If the `<host>` tag is omitted, then the `<listen_host>` from the root config is
used.

### Configuring layer sequences {#layers-sequence-is-defined-by-impl-tag-referencing-another-module}

Layers sequences are defined using the `<impl>` tag, and referencing another 
module. For example, to configure a TLS layer on top of our plain_http module
we could further modify our configuration as follows:

```xml
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

### Attaching endpoints to layers {#endpoint-can-be-attached-to-any-layer}

Endpoints can be attached to any layer. For example, we can define endpoints for
HTTP (port 8123) and HTTPS (port 8443):

```xml
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

### Defining additional endpoints {#additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag}

Additional endpoints can be defined by referencing any module and omitting the 
`<type>` tag. For example, we can define `another_http` endpoint for the 
`plain_http` module as follows:

```xml
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

### Specifying additional layer parameters {#some-modules-can-contain-specific-for-its-layer-parameters}

Some modules can contain additional layer parameters. For example, the TLS layer
allows a private key (`privateKeyFile`) and certificate files (`certificateFile`)
to be specified as follows:

```xml
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
