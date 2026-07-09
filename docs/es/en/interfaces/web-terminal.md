---
description: 'Documentación del terminal web, una sesión de `clickhouse-client` en el navegador a través de WebSocket'
sidebar_label: 'Terminal web'
sidebar_position: 22
slug: /interfaces/web-terminal
title: 'Terminal web'
doc_type: 'reference'
---

El terminal web es una interfaz en el navegador que proporciona una sesión interactiva de `clickhouse-client` a través de WebSocket. Está disponible en cualquier puerto HTTP de ClickHouse en la ruta `/webterminal`.

Vaya a `/webterminal` en cualquier puerto HTTP de ClickHouse (por ejemplo, `http://localhost:8123/webterminal`) para abrir el terminal.

<div id="enabling-the-feature">
  ## Habilitación y deshabilitación de la funcionalidad
</div>

El endpoint `/webterminal` está habilitado de forma predeterminada y se controla mediante la configuración del servidor `enable_webterminal`. Para deshabilitarlo, establezca la configuración en `false`; las solicitudes a `/webterminal` devolverán el código de estado HTTP `403 Forbidden`.

```xml
<clickhouse>
    <enable_webterminal>false</enable_webterminal>
</clickhouse>
```

:::note
`enable_webterminal` sustituye al ajuste anterior `allow_experimental_webterminal`. El nombre antiguo sigue siendo compatible por compatibilidad con versiones anteriores cuando no se ha establecido `enable_webterminal`.
:::

<div id="authentication">
  ## Autenticación
</div>

El terminal web autentica al usuario mediante las mismas comprobaciones de `Session` y de control de acceso que el protocolo HTTP, pero las credenciales se intercambian en banda a través de la conexión WebSocket ya establecida, en lugar de mediante la solicitud HTTP de cambio de protocolo. Una vez completado el handshake de WebSocket, el navegador envía el primer mensaje como JSON:

```json
{"type": "auth", "user": "<user>", "password": "<password>"}
```

Esto evita poner credenciales en parámetros de URL o en cabeceras `Authorization` adjuntas a la solicitud de actualización, donde podrían acabar en el historial del navegador, los logs de acceso del servidor y los logs del proxy inverso. `/webterminal` **no** consulta intencionadamente los parámetros de URL, HTTP Basic ni las cabeceras `X-ClickHouse-User`/`X-ClickHouse-Key` de la solicitud de actualización.

Si las credenciales no son válidas, el servidor cierra el WebSocket con el código `1008`; la interfaz del navegador vuelve a solicitar las credenciales.

<div id="session">
  ## Cómo es la sesión
</div>

Una vez autenticado, el servidor ejecuta `clickhouse-client` conectado a un pseudoterminal y redirige su entrada y salida a través de WebSocket. La sesión ofrece toda la experiencia de `clickhouse-client`, incluida:

* Resaltado de sintaxis.
* Autocompletado.
* Consultas de varias líneas.
* Historial de comandos (almacenado en el servidor mientras dura la sesión).

La terminal usa [xterm.js](https://xtermjs.org/) para el renderizado. Todos los recursos se sirven desde el propio binario de ClickHouse; no se carga ninguna CDN de terceros.

<div id="play-integration">
  ## Integración con `/play`
</div>

La UI web SQL [`/play`](/es/interfaces/http) incorpora la terminal web como un panel acoplable. Actívala o desactívala con el icono de terminal de la barra lateral, o pulsa la tecla `~` cuando el editor de consultas esté vacío. La página `/play` detecta la disponibilidad de `/webterminal` al cargarse y oculta los controles de la terminal cuando el endpoint no está disponible (por ejemplo, cuando `enable_webterminal` está establecido en `false`).

<div id="security">
  ## Consideraciones de seguridad
</div>

El terminal web expone una sesión interactiva similar a una shell a cualquier persona que pueda autenticarse en el endpoint HTTP de ClickHouse, por lo que aquí se aplican las mismas advertencias que al protocolo HTTP:

* Sirva siempre `/webterminal` a través de HTTPS en entornos no confiables para proteger las credenciales y el tráfico de la sesión.
* Restrinja el acceso a nivel de red (`firewall`, `reverse proxy` o la configuración `listen_host`) del mismo modo que restringe el acceso al protocolo HTTP.
* El endpoint valida el encabezado `Origin` frente a `Host` para mitigar el secuestro de WebSocket entre orígenes; configure los proxies inversos en consecuencia si termina TLS de forma externa.
* Detrás de un proxy inverso que termina TLS, la conexión upstream a ClickHouse es `http` sin cifrar aunque el navegador use `https`, por lo que la comprobación estricta del mismo origen rechazaría conexiones legítimas. Para estas implementaciones, establezca `webterminal_allowed_origins` como una lista separada por comas de orígenes completos a los que se permite abrir sesiones de WebSocket; cuando esta configuración no está vacía, sustituye la comprobación predeterminada del mismo origen. Ejemplo: `<webterminal_allowed_origins>https://example.com,https://app.example.com:8443</webterminal_allowed_origins>`.

El handler también exige la conformidad con el protocolo WebSocket según la RFC 6455: las tramas de cliente sin enmascarar, los códigos de operación reservados, las tramas de control demasiado grandes o fragmentadas y los bits RSV reservados se rechazan con códigos de cierre por error de protocolo.

<div id="platform">
  ## Disponibilidad de la plataforma
</div>

El handler se compila en todas las plataformas compatibles con ClickHouse. La capa de pseudoterminal utilizada por el ejecutor integrado de `clickhouse-client` está implementada sobre primitivas POSIX portables (`posix_openpt`/`grantpt`/`unlockpt`), con una implementación específica para Linux que usa `ptsname_r`, seguro para subproceso. Los enlaces a `/webterminal` en la página de inicio de ClickHouse y en `/play` se ocultan automáticamente cuando el endpoint no está disponible (por ejemplo, cuando `enable_webterminal` se establece en `false`).