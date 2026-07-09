---
description: 'Documentación de HTTP'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

El servidor HTTP puede usarse para autenticar a los usuarios de ClickHouse. La autenticación HTTP solo puede usarse como autenticador externo para usuarios existentes, definidos en `users.xml` o en rutas locales de control de acceso. Actualmente, se admite el esquema de autenticación [Basic](https://datatracker.ietf.org/doc/html/rfc7617) mediante el método GET.

<div id="http-auth-server-definition">
  ## Definición del servidor de autenticación HTTP
</div>

Para definir un servidor de autenticación HTTP, debe añadir la sección `http_authentication_servers` al archivo `config.xml`.

**Ejemplo**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

Tenga en cuenta que puede definir varios servidores HTTP dentro de la sección `http_authentication_servers` con nombres distintos.

**Parámetros**

* `uri` - URI para realizar la solicitud de autenticación

Tiempos de espera, en milisegundos, en el socket utilizado para comunicarse con el servidor:

* `connection_timeout_ms` - Predeterminado: 1000 ms.
* `receive_timeout_ms` - Predeterminado: 1000 ms.
* `send_timeout_ms` - Predeterminado: 1000 ms.

Parámetros de reintento:

* `max_tries` - Número máximo de intentos para realizar una solicitud de autenticación. Predeterminado: 3
* `retry_initial_backoff_ms` - Intervalo inicial de backoff en caso de reintento. Predeterminado: 50 ms
* `retry_max_backoff_ms` - Intervalo máximo de backoff. Predeterminado: 1000 ms

Headers reenviados:

Esta parte define qué headers se redirigirán desde los headers de la solicitud del client al autenticador HTTP externo. Tenga en cuenta que los headers se compararán con los de la configuración sin distinguir entre mayúsculas y minúsculas, pero se redirigirán tal cual, es decir, sin modificaciones.

<div id="enabling-http-auth-in-users-xml">
  ### Habilitación de la autenticación HTTP en `users.xml`
</div>

Para habilitar la autenticación HTTP para el usuario, especifique la sección `http_authentication` en lugar de `password` o de secciones similares en la definición del usuario.

Parámetros:

* `server` - Nombre del servidor de autenticación HTTP configurado en el archivo `config.xml` principal, como se describió anteriormente.
* `scheme` - Esquema de autenticación HTTP. Por ahora, solo se admite `Basic`. Valor predeterminado: Basic

Ejemplo (va en `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
Tenga en cuenta que la autenticación HTTP no puede utilizarse junto con ningún otro mecanismo de autenticación. La presencia de cualquier otra sección, como `password`, junto con `http_authentication`, hará que ClickHouse se cierre.
:::

<div id="enabling-http-auth-using-sql">
  ### Habilitar la autenticación HTTP mediante SQL
</div>

Cuando [Control de acceso y gestión de cuentas basado en SQL](/es/operations/access-rights#access-control-usage) está habilitado en ClickHouse, también se pueden crear con sentencias SQL los usuarios identificados mediante autenticación HTTP.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...o bien, `Basic` se usa por defecto sin especificar explícitamente el esquema

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### Pasar configuraciones de sesión
</div>

Si el cuerpo de la respuesta del servidor de autenticación HTTP tiene formato JSON y contiene el subobjeto `settings`, ClickHouse intentará analizar sus pares clave-valor como valores de cadena y establecerlos como configuraciones de sesión para la sesión actual del usuario autenticado. Si el análisis falla, se ignorará el cuerpo de la respuesta del servidor.