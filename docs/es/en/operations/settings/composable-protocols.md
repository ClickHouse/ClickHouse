---
description: 'Los protocolos componibles permiten una configuración más flexible del acceso TCP
  al servidor de ClickHouse.'
sidebar_label: 'Protocolos componibles'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Protocolos componibles'
doc_type: 'reference'
---

<div id="overview">
  ## Descripción general
</div>

Los protocolos componibles permiten una configuración más flexible del acceso TCP al
servidor de ClickHouse. Esta configuración puede coexistir con la
configuración convencional o sustituirla.

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## Configuración de protocolos componibles
</div>

Los protocolos componibles pueden configurarse en un archivo de configuración XML. La sección de protocolos
se identifica mediante las etiquetas `protocols` en el archivo de configuración XML:

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### Configuración de capas de protocolo
</div>

Puede definir capas de protocolo con módulos básicos. Por ejemplo, para definir una
capa HTTP, puede añadir un nuevo módulo básico a la sección `protocols`:

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

Los módulos se pueden configurar de la siguiente manera:

* `plain_http` - nombre al que puede hacer referencia otra capa
* `type` - indica el handler de protocolo que se instanciará para procesar datos.
  Tiene el siguiente conjunto de handlers de protocolo predefinidos:
  * `tcp` - handler del protocolo nativo de ClickHouse
  * `http` - handler del protocolo HTTP de ClickHouse
  * `tls` - capa de cifrado TLS
  * `proxy1` - capa PROXYv1
  * `mysql` - handler del protocolo de compatibilidad con MySQL
  * `postgres` - handler del protocolo de compatibilidad con PostgreSQL
  * `prometheus` - handler del protocolo Prometheus
  * `interserver` - handler de interservidor de ClickHouse

:::note
El handler del protocolo `gRPC` no está implementado para `protocolos componibles`
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### Configuración de endpoints
</div>

Los endpoints (puertos de escucha) se indican mediante las etiquetas `<port>` y la etiqueta opcional `<host>`.
Por ejemplo, para configurar un endpoint en la capa HTTP añadida anteriormente,
podríamos modificar nuestra configuración de la siguiente manera:

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

Si se omite la etiqueta `<host>`, se usa el valor de `<listen_host>` de la configuración raíz.

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### Configuración de secuencias de capas
</div>

Las secuencias de capas se definen con la etiqueta `<impl>` haciendo referencia a otro
módulo. Por ejemplo, para configurar una capa TLS sobre nuestro módulo plain&#95;http
podríamos seguir modificando nuestra configuración de la siguiente manera:

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

<div id="endpoint-can-be-attached-to-any-layer">
  ### Asociar endpoints a las capas
</div>

Los endpoints se pueden asociar a cualquier capa. Por ejemplo, podemos definir endpoints para
HTTP (puerto 8123) y HTTPS (puerto 8443):

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

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### Definir endpoints adicionales
</div>

Se pueden definir endpoints adicionales haciendo referencia a cualquier módulo y omitiendo la
etiqueta `<type>`. Por ejemplo, podemos definir el endpoint `another_http` para el
módulo `plain_http` de la siguiente manera:

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

<div id="custom-http-handlers-per-endpoint">
  ### Handlers HTTP personalizados por endpoint
</div>

De forma predeterminada, todas las entradas del protocolo `type=http` comparten la misma
configuración `<http_handlers>`. Puede sobrescribir esto añadiendo una etiqueta `<handlers>` que apunte
a una sección de configuración distinta. Esto permite que cada puerto HTTP sirva
un conjunto diferente de reglas de enrutamiento HTTP.

Por ejemplo, para ejecutar una API HTTP alternativa en el puerto 8124 con sus propios handlers:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

En este ejemplo, las solicitudes al puerto 8123 usan las reglas estándar de `<http_handlers>`,
mientras que las solicitudes al puerto 8124 usan las reglas de `<http_handlers_alt>`. Si se omite `<handlers>`,
el endpoint recurre al valor predeterminado `<http_handlers>`.

La sección de handlers personalizados sigue el mismo formato que
[`<http_handlers>`](/es/docs/operations/server-configuration-parameters/settings#http_handlers).
Los cambios en la sección de handlers personalizados se detectan durante la recarga de la configuración, y el
endpoint correspondiente se reinicia automáticamente.

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### Especificar parámetros adicionales de capa
</div>

Algunos módulos pueden incluir parámetros adicionales de capa. Por ejemplo, la capa TLS
permite especificar una clave privada (`privateKeyFile`) y archivos de certificado (`certificateFile`)
de la siguiente manera:

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