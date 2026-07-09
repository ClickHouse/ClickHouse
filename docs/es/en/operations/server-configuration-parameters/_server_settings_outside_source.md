---
title: Configuración del servidor fuera de la fuente
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

Está habilitado de forma predeterminada en ClickHouse Cloud.

Si esta configuración no está habilitada de forma predeterminada en su entorno, según cómo se haya instalado ClickHouse, puede seguir las instrucciones que aparecen a continuación para habilitarla o deshabilitarla.

**Habilitación**

Para activar manualmente la recopilación del historial de métricas asíncronas [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md), cree `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` con el siguiente contenido:

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**Desactivación**

Para desactivar la configuración `asynchronous_metric_log`, debe crear el siguiente archivo `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` con el siguiente contenido:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

Usar la dirección de origen para la autenticación de los clientes conectados a través de un proxy.

:::note
Esta configuración debe usarse con especial precaución, ya que las direcciones reenviadas pueden falsificarse fácilmente. No se debe acceder directamente a los servidores que aceptan este tipo de autenticación, sino exclusivamente a través de un proxy de confianza.
:::

<div id="backups">
  ## copias de seguridad
</div>

Configuración de las copias de seguridad, utilizada al ejecutar las sentencias [`BACKUP` y `RESTORE`](/es/operations/backup/overview).

La siguiente configuración puede establecerse mediante subetiquetas:

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','Determina si varias operaciones de copia de seguridad pueden ejecutarse de forma concurrente en el mismo host.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'Determina si varias operaciones de restauración pueden ejecutarse de forma concurrente en el mismo host.', 'true'),
    ('allowed_disk', 'String', 'Disco de destino para la copia de seguridad al usar `File()`. Esta configuración debe establecerse para poder usar `File`.', ''),
    ('allowed_path', 'String', 'Ruta de destino para la copia de seguridad al usar `File()`. Esta configuración debe establecerse para poder usar `File`.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'Número de intentos para recopilar metadatos antes de hacer una pausa en caso de inconsistencia tras comparar los metadatos recopilados.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'Tiempo de espera en milisegundos para recopilar metadatos durante la copia de seguridad.', '600000'),
    ('compare_collected_metadata', 'Bool', 'Si es `true`, compara los metadatos recopilados con los metadatos existentes para garantizar que no cambien durante la copia de seguridad.', 'true'),
    ('create_table_timeout', 'UInt64', 'Tiempo de espera en milisegundos para crear tablas durante la restauración.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'Número máximo de reintentos tras encontrar un error de versión no válida durante la copia de seguridad/restauración coordinada.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Tiempo máximo de pausa en milisegundos antes del siguiente intento de recopilar metadatos.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Tiempo mínimo de pausa en milisegundos antes del siguiente intento de recopilar metadatos.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'Si el comando `BACKUP` falla, ClickHouse intentará eliminar los archivos ya copiados en la copia de seguridad antes del fallo; de lo contrario, dejará los archivos copiados tal como están.', 'true'),
    ('sync_period_ms', 'UInt64', 'Período de sincronización en milisegundos para la copia de seguridad/restauración coordinada.', '5000'),
    ('test_inject_sleep', 'Bool', 'Pausa relacionada con pruebas', 'false'),
    ('test_randomize_order', 'Bool', 'Si es `true`, aleatoriza el orden de determinadas operaciones con fines de prueba.', 'false'),
    ('zookeeper_path', 'String', 'Ruta en ZooKeeper donde se almacenan los metadatos de copia de seguridad y restauración al usar la cláusula `ON CLUSTER`.', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
  */ }

| Configuración                                       | Tipo   | Descripción                                                                                                                                                                                  | Predeterminado        |
| :-------------------------------------------------- | :----- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | Determina si varias operaciones de copia de seguridad pueden ejecutarse de forma concurrente en el mismo host.                                                                               | `true`                |
| `allow_concurrent_restores`                         | Bool   | Determina si varias operaciones de restauración pueden ejecutarse de forma concurrente en el mismo host.                                                                                     | `true`                |
| `allowed_disk`                                      | String | Disco al que se realizará la copia de seguridad al usar `File()`. Esta configuración debe establecerse para poder usar `File`.                                                               | &#96;&#96;            |
| `allowed_path`                                      | String | Ruta en la que se realizará la copia de seguridad al usar `File()`. Esta configuración debe establecerse para poder usar `File`.                                                             | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | Número de intentos para recopilar metadatos antes de hacer una pausa en caso de inconsistencias tras comparar los metadatos recopilados.                                                     | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | Tiempo de espera en milisegundos para recopilar metadatos durante la copia de seguridad.                                                                                                     | `600000`              |
| `compare_collected_metadata`                        | Bool   | Si es `true`, compara los metadatos recopilados con los metadatos existentes para asegurarse de que no cambien durante la copia de seguridad.                                                | `true`                |
| `create_table_timeout`                              | UInt64 | Tiempo de espera en milisegundos para crear tablas durante la restauración.                                                                                                                  | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | Número máximo de intentos de reintento tras encontrarse con un error de versión no válida durante la copia de seguridad/restauración coordinada.                                             | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Tiempo máximo de pausa en milisegundos antes del siguiente intento de recopilar metadatos.                                                                                                   | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Tiempo mínimo de pausa en milisegundos antes del siguiente intento de recopilar metadatos.                                                                                                   | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | Si el comando `BACKUP` falla, ClickHouse intentará eliminar los archivos ya copiados en la copia de seguridad antes del fallo; de lo contrario, dejará los archivos copiados tal como están. | `true`                |
| `sync_period_ms`                                    | UInt64 | Período de sincronización en milisegundos para la copia de seguridad/restauración coordinada.                                                                                                | `5000`                |
| `test_inject_sleep`                                 | Bool   | Pausa para pruebas                                                                                                                                                                           | `false`               |
| `test_randomize_order`                              | Bool   | Si es `true`, aleatoriza el orden de ciertas operaciones con fines de prueba.                                                                                                                | `false`               |
| `zookeeper_path`                                    | String | Ruta en ZooKeeper donde se almacenan los metadatos de copia de seguridad y restauración al usar la cláusula `ON CLUSTER`.                                                                    | `/clickhouse/backups` |

Esta configuración se establece de forma predeterminada así:

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

Contiene información sobre todas las tareas en segundo plano que se ejecutan a través de varios pools en segundo plano.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

Factor de trabajo para el tipo de autenticación `bcrypt_password`, que utiliza el [algoritmo Bcrypt](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
El factor de trabajo define la cantidad de cálculos y el tiempo necesarios para calcular el hash y verificar la contraseña.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
Para aplicaciones con autenticación frecuente,
considere métodos de autenticación alternativos debido a la
carga computacional de bcrypt con factores de trabajo elevados.
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

Si se establece en true, los usuarios necesitan un grant para crear una tabla con un motor específico; por ejemplo, `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
De forma predeterminada, por compatibilidad con versiones anteriores, al crear una tabla con un motor de tabla específico se ignora el grant; no obstante, puede cambiar este comportamiento estableciendo este valor en true.
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

Intervalo en segundos antes de recargar los diccionarios integrados.

ClickHouse recarga los diccionarios integrados cada x segundos. Esto permite editar los diccionarios &quot;al vuelo&quot; sin reiniciar el servidor.

**Ejemplo**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## compresión
</div>

Configuración de compresión de datos para tablas con motor [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

:::note
Te recomendamos no cambiar esto si acabas de empezar a usar ClickHouse.
:::

**Plantilla de configuración**:

```xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

**Campos de `<case>`**:

* `min_part_size` – El tamaño mínimo de una parte de datos.
* `min_part_size_ratio` – La proporción entre el tamaño de la parte de datos y el tamaño de la tabla.
* `method` – Método de compresión. Valores aceptados: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
* `level` – Nivel de compresión. Consulta [Codecs](/es/sql-reference/statements/create/table#general-purpose-codecs).

:::note
Puedes configurar varias secciones `<case>`.
:::

**Acciones cuando se cumplen las condiciones**:

* Si una parte de datos coincide con un conjunto de condiciones, ClickHouse usa el método de compresión especificado.
* Si una parte de datos coincide con varios conjuntos de condiciones, ClickHouse usa el primer conjunto de condiciones que coincida.

:::note
Si no se cumple ninguna condición para una parte de datos, ClickHouse usa la compresión `lz4`.
:::

**Ejemplo**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

<div id="encryption">
  ## cifrado
</div>

Configura un comando para obtener una clave que se usará con los [códecs de cifrado](/es/sql-reference/statements/create/table#encryption-codecs). La clave (o las claves) debe escribirse en variables de entorno o establecerse en el archivo de configuración.

Las claves pueden ser hexadecimales o cadenas con una longitud de 16 bytes.

**Ejemplo**

Carga desde la configuración:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
No se recomienda almacenar las claves en el archivo de configuración. No es una práctica segura. Puede mover las claves a un archivo de configuración independiente en un disco seguro y colocar un enlace simbólico a ese archivo de configuración en la carpeta `config.d/`.
:::

Carga desde la configuración, cuando la clave está en hexadecimal:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Cargando la clave desde la variable de entorno:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Aquí, `current_key_id` establece la clave actual para el cifrado, y todas las claves especificadas pueden usarse para el descifrado.

Cada uno de estos métodos puede aplicarse a varias claves:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Aquí, `current_key_id` muestra la clave actual de cifrado.

Además, los usuarios pueden añadir un nonce que debe tener una longitud de 12 bytes (de forma predeterminada, los procesos de cifrado y descifrado usan un nonce compuesto por bytes con valor cero):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

O puede indicarse en hexadecimal:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Todo lo mencionado anteriormente puede aplicarse a `aes_256_gcm_siv` (pero la clave debe tener una longitud de 32 bytes).
:::

<div id="error_log">
  ## error_log
</div>

Está desactivado de forma predeterminada.

**Activación**

Para activar manualmente la recopilación del historial de errores [`system.error_log`](../../operations/system-tables/error_log.md), cree `/etc/clickhouse-server/config.d/error_log.xml` con el siguiente contenido:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**Desactivar**

Para desactivar la configuración `error_log`, debe crear el siguiente archivo `/etc/clickhouse-server/config.d/disable_error_log.xml` con el siguiente contenido:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

Lista de prefijos utilizados para los [ajustes personalizados](/es/operations/settings/query-level#custom_settings).
Si hay varios prefijos, deben separarse con comas.

**Ejemplo**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**Véase también**

* [Ajustes personalizados](/es/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

Configura el límite flexible para el tamaño del archivo de core dump.

:::note
El límite duro se configura mediante herramientas del sistema
:::

**Ejemplo**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

Perfil de configuración predeterminado. Los perfiles de configuración se encuentran en el archivo especificado en el ajuste `user_config`.

**Ejemplo**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

Ruta del archivo de configuración de los diccionarios.

Ruta:

* Especifique la ruta absoluta o la ruta relativa al archivo de configuración del servidor.
* La ruta puede contener comodines * y ?.

Véase también:

* &quot;[Diccionarios](../../sql-reference/statements/create/dictionary/overview.md)&quot;.

**Ejemplo**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

La ruta del archivo de configuración de las funciones ejecutables definidas por el usuario.

Ruta:

* Especifique la ruta absoluta o una ruta relativa al archivo de configuración del servidor.
* La ruta puede contener comodines * y ?.

Véase también:

* &quot;[Executable User Defined Functions](/es/sql-reference/functions/udf#executable-user-defined-functions).&quot;.

**Ejemplo**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

Envío de datos a [Graphite](https://github.com/graphite-project).

Configuración:

* `host` – El servidor de Graphite.
* `port` – El puerto del servidor de Graphite.
* `interval` – El intervalo de envío, en segundos.
* `timeout` – El tiempo de espera para el envío de datos, en segundos.
* `root_path` – Prefijo para las claves.
* `metrics` – Envío de datos desde la tabla [system.metrics](/es/operations/system-tables/metrics).
* `events` – Envío de datos delta acumulados durante el período de tiempo desde la tabla [system.events](/es/operations/system-tables/events).
* `events_cumulative` – Envío de datos acumulados desde la tabla [system.events](/es/operations/system-tables/events).
* `asynchronous_metrics` – Envío de datos desde la tabla [system.asynchronous&#95;metrics](/es/operations/system-tables/asynchronous_metrics).

Se pueden configurar varias cláusulas `<graphite>`. Por ejemplo, puede usar esto para enviar distintos datos en distintos intervalos.

**Ejemplo**

```xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

<div id="graphite_rollup">
  ## graphite_rollup
</div>

Configuración para reducir la granularidad de los datos de Graphite.

Para obtener más detalles, consulte [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Ejemplo**

```xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

<div id="http_handlers">
  ## http_handlers
</div>

Permite usar handlers HTTP personalizados.
Para añadir un nuevo handler `http`, basta con agregar una nueva `<rule>`.
Las reglas se comprueban de arriba abajo, tal como se definen,
y la primera coincidencia ejecutará el handler.
Una regla sin condiciones de coincidencia (solo `handler`) coincide con todas las solicitudes; como las reglas se comprueban en orden,
una regla así solo resulta útil como fallback si se coloca al final.

Los siguientes ajustes pueden configurarse mediante subetiquetas (todas estas subetiquetas son opcionales excepto `handler`):

| Sub-tags             | Definition                                                                                                                                                                                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | Para hacer coincidir la ruta URL de la solicitud. La query string se ignora al realizar la coincidencia                                                                                                                                                                                                                 |
| `url_prefix`         | Para hacer coincidir la ruta URL de la solicitud con una ruta base: la propia ruta o cualquier elemento que cuelgue de ella en un límite de segmento de ruta (p. ej., &#39;/api/v1&#39; coincide con /api/v1, /api/v1/ y /api/v1/write, pero no con /api/v1beta). La query string se ignora al realizar la coincidencia |
| `url_regexp`         | Para hacer coincidir la ruta URL de la solicitud con una expresión regular. La query string se ignora al realizar la coincidencia                                                                                                                                                                                       |
| `full_url`           | Para hacer coincidir la URL completa de la solicitud `scheme://host:port/path`. La query string se ignora al realizar la coincidencia, y el host es la dirección IP de la conexión (no el header `Host`)                                                                                                                |
| `full_url_prefix`    | Para hacer coincidir la URL completa de la solicitud `scheme://host:port/path` con la base URL `scheme://host:port/base_path`, en un límite de segmento de ruta (consulte `url_prefix`). La query string se ignora al realizar la coincidencia                                                                          |
| `full_url_regexp`    | Para hacer coincidir la URL completa de la solicitud `scheme://host:port/path` con una expresión regular. La query string se ignora al realizar la coincidencia                                                                                                                                                         |
| `methods`            | Para hacer coincidir métodos de solicitud, puede usar comas para separar varias coincidencias de método                                                                                                                                                                                                                 |
| `headers`            | Para hacer coincidir headers de solicitud, haga coincidir cada elemento hijo (el nombre del elemento hijo es el nombre del header)                                                                                                                                                                                      |
| `headers_regexp`     | Igual que `headers`, pero el valor de cada elemento hijo se compara con una expresión regular                                                                                                                                                                                                                           |
| `empty_query_string` | Comprueba que no haya query string en la URL                                                                                                                                                                                                                                                                            |
| `handler`            | El handler de la solicitud (obligatorio)                                                                                                                                                                                                                                                                                |

:::note
En lugar de `url_regexp`, `full_url_regexp` y `headers_regexp`, también puede escribir una expresión regular en `url`, `full_url` o `headers` usando el prefijo `regex:` (p. ej., `<url>regex:/api/.*</url>`). Esto sigue siendo compatible por compatibilidad con versiones anteriores, pero está obsoleto: prefiera las subetiquetas específicas `url_regexp`, `full_url_regexp` y `headers_regexp`.
:::

`handler` contiene los siguientes ajustes, que pueden configurarse mediante subetiquetas:

| Sub-tags           | Definition                                                                                                                                                                                                      |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | Una ubicación para la redirección                                                                                                                                                                               |
| `type`             | Tipos compatibles: static, dynamic&#95;query&#95;handler, predefined&#95;query&#95;handler, redirect                                                                                                            |
| `status`           | Se usa con el tipo static, código de estado de la respuesta                                                                                                                                                     |
| `query_param_name` | Se usa con el tipo dynamic&#95;query&#95;handler, extrae y ejecuta el valor correspondiente al valor de `<query_param_name>` en los params de la solicitud HTTP                                                 |
| `query`            | Se usa con el tipo predefined&#95;query&#95;handler, ejecuta la consulta cuando se llama al handler                                                                                                             |
| `content_type`     | Se usa con el tipo static, content-type de la respuesta                                                                                                                                                         |
| `response_content` | Se usa con el tipo static, contenido de la respuesta enviado al client; al usar el prefijo &#39;file://&#39; o &#39;config://&#39;, obtiene el contenido del archivo o de la configuración y lo envía al client |

Junto con una lista de reglas, puede especificar `<defaults/>`, lo que habilita todos los handlers predeterminados.

Ejemplo:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

<div id="http_server_default_response">
  ## http_server_default_response
</div>

La página que se muestra de forma predeterminada al acceder al servidor HTTP(s) de ClickHouse.
El valor predeterminado es &quot;Ok.&quot; (con un salto de línea al final)

**Ejemplo**

Abre `https://tabix.io/` al acceder a `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

Se utiliza para añadir headers a la respuesta en una solicitud HTTP `OPTIONS`.
El método `OPTIONS` se usa al realizar solicitudes preflight de CORS.

Para obtener más información, consulta [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

Ejemplo:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

<div id="hsts_max_age">
  ## hsts_max_age
</div>

Tiempo de expiración de HSTS en segundos.

:::note
Un valor de `0` significa que ClickHouse desactiva HSTS. Si establece un número positivo, HSTS se habilitará y `max-age` será el número que haya establecido.
:::

**Ejemplo**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

Restricción de los hosts que pueden intercambiar datos entre servidores de ClickHouse.
Si se usa Keeper, la misma restricción se aplicará a la comunicación entre distintas instancias de Keeper.

:::note
De forma predeterminada, el valor es igual al ajuste [`listen_host`](#listen_host).
:::

**Ejemplo**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

Tipo:

Valor predeterminado:

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

Un nombre de usuario y una contraseña que se utilizan para conectarse a otros servidores durante la [replicación](../../engines/table-engines/mergetree-family/replication.md). Además, el servidor autentica a otras réplicas mediante estas credenciales.
Por lo tanto, `interserver_http_credentials` debe ser el mismo para todas las réplicas de un clúster.

:::note

* De forma predeterminada, si se omite la sección `interserver_http_credentials`, no se utiliza autenticación durante la replicación.
* La configuración de `interserver_http_credentials` no está relacionada con la [configuración](../../interfaces/client.md#configuration_files) de credenciales de un Client de ClickHouse.
* Estas credenciales son comunes para la replicación mediante `HTTP` y `HTTPS`.
  :::

Las siguientes opciones de configuración pueden establecerse mediante subetiquetas:

* `user` — Nombre de usuario.
* `password` — Contraseña.
* `allow_empty` — Si es `true`, se permite que otras réplicas se conecten sin autenticación aunque se hayan configurado credenciales. Si es `false`, se rechazan las conexiones sin autenticación. Valor predeterminado: `false`.
* `old` — Contiene el `user` y el `password` antiguos utilizados durante la rotación de credenciales. Se pueden especificar varias secciones `old`.

**Rotación de credenciales**

ClickHouse admite la rotación dinámica de credenciales entre servidores sin detener todas las réplicas al mismo tiempo para actualizar su configuración. Las credenciales pueden cambiarse en varios pasos.

Para habilitar la autenticación, establezca `interserver_http_credentials.allow_empty` en `true` y agregue credenciales. Esto permite conexiones con autenticación y sin ella.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

Después de configurar todas las réplicas, establezca `allow_empty` en `false` o elimine esta configuración. Esto hace obligatoria la autenticación con las nuevas credenciales.

Para cambiar las credenciales existentes, mueva el nombre de usuario y la contraseña a la sección `interserver_http_credentials.old` y actualice `user` y `password` con los nuevos valores. En este punto, el servidor usa las nuevas credenciales para conectarse a las demás réplicas y acepta conexiones tanto con las credenciales nuevas como con las antiguas.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

Cuando las nuevas credenciales se hayan aplicado en todas las réplicas, podrán eliminarse las credenciales antiguas.

<div id="ldap_servers">
  ## ldap_servers
</div>

Enumere aquí los servidores LDAP con sus parámetros de conexión para:

* usarlos como autenticadores para usuarios locales específicos, que tienen configurado el mecanismo de autenticación `ldap` en lugar de `password`
* usarlos como directorios de usuarios remotos.

Los siguientes ajustes pueden configurarse mediante subetiquetas:

| Configuración                  | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bind_dn`                      | Plantilla utilizada para construir el DN con el que se realizará el bind. El DN resultante se construirá reemplazando todas las subcadenas `\{user_name\}` de la plantilla por el nombre de usuario real en cada intento de autenticación.                                                                                                                                                                                                                                                                              |
| `enable_tls`                   | Indicador para activar el uso de una conexión segura con el servidor LDAP. Especifique `no` para el protocolo de texto sin formato (`ldap://`) (no recomendado). Especifique `yes` para el protocolo LDAP sobre SSL/TLS (`ldaps://`) (recomendado y valor predeterminado). Especifique `starttls` para el protocolo StartTLS heredado (protocolo de texto sin formato (`ldap://`) actualizado a TLS).                                                                                                                   |
| `host`                         | Nombre de host o IP del servidor LDAP; este parámetro es obligatorio y no puede estar vacío.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `port`                         | Puerto del servidor LDAP; el valor predeterminado es 636 si `enable_tls` está establecido en true; en caso contrario, `389`.                                                                                                                                                                                                                                                                                                                                                                                            |
| `tls_ca_cert_dir`              | ruta al directorio que contiene los certificados de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_ca_cert_file`             | ruta al archivo del certificado de CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `tls_cert_file`                | ruta al archivo del certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `tls_cipher_suite`             | conjunto de cifrado permitido (en notación de OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_key_file`                 | ruta al archivo de la clave del certificado.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `tls_minimum_protocol_version` | La versión mínima del protocolo SSL/TLS. Los valores aceptados son: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (valor predeterminado).                                                                                                                                                                                                                                                                                                                                                                                |
| `tls_require_cert`             | Comportamiento de la verificación del certificado del par SSL/TLS. Los valores aceptados son: `never`, `allow`, `try`, `demand` (valor predeterminado).                                                                                                                                                                                                                                                                                                                                                                 |
| `user_dn_detection`            | Sección con parámetros de búsqueda LDAP para detectar el DN real del usuario vinculado. Esto se usa principalmente en filtros de búsqueda para una posterior asignación de roles cuando el servidor es Active Directory. El DN de usuario resultante se usará al reemplazar las subcadenas `\{user_dn\}` allí donde estén permitidas. De forma predeterminada, el DN de usuario se establece igual que el DN de bind, pero una vez realizada la búsqueda, se actualizará con el valor real detectado del DN de usuario. |
| `verification_cooldown`        | Un período de tiempo, en segundos, después de un intento de bind correcto, durante el cual se asumirá que el usuario se ha autenticado correctamente para todas las solicitudes consecutivas sin contactar con el servidor LDAP. Especifique `0` (valor predeterminado) para deshabilitar el almacenamiento en caché y forzar el contacto con el servidor LDAP en cada solicitud de autenticación.                                                                                                                      |

El ajuste `user_dn_detection` puede configurarse con subetiquetas:

| Configuración   | Descripción                                                                                                                                                                                                                                                                                                                                                                   |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | plantilla utilizada para construir el DN base de la búsqueda LDAP. El DN resultante se construirá reemplazando todas las subcadenas `\{user_name\}` y `\{bind_dn\}` de la plantilla por el nombre de usuario real y el DN de bind durante la búsqueda LDAP.                                                                                                                   |
| `scope`         | alcance de la búsqueda LDAP. Los valores aceptados son: `base`, `one_level`, `children`, `subtree` (valor predeterminado).                                                                                                                                                                                                                                                    |
| `search_filter` | plantilla utilizada para construir el filtro de búsqueda de LDAP. El filtro resultante se construirá reemplazando todas las subcadenas `\{user_name\}`, `\{bind_dn\}` y `\{base_dn\}` de la plantilla por el nombre de usuario real, el DN de bind y el DN base durante la búsqueda LDAP. Tenga en cuenta que los caracteres especiales deben escaparse correctamente en XML. |

Ejemplo:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

Ejemplo (Active Directory típico con detección del DN de usuario configurada para una posterior asignación de roles):

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

<div id="listen_host">
  ## listen_host
</div>

Restricción de los hosts desde los que pueden llegar las solicitudes. Si quiere que el servidor responda a todos ellos, especifique `::`.

Ejemplos:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## logger
</div>

La ubicación y el formato de los mensajes de log.

**Claves**:

| Key                          | Description                                                                                                                                                                                                                                                                                                                                         |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async`                      | Cuando es `true` (valor predeterminado), el logging se realiza de forma asíncrona (un hilo en segundo plano por canal de salida). De lo contrario, se registrará dentro del hilo que llama a LOG                                                                                                                                                    |
| `async_queue_max_size`       | Al usar logging asíncrono, es la cantidad máxima de mensajes que se conservarán en la cola a la espera de vaciarse. Los mensajes adicionales se descartarán                                                                                                                                                                                         |
| `console`                    | Habilita el logging en la consola. Establézcalo en `1` o `true` para activarlo. El valor predeterminado es `1` si ClickHouse no se ejecuta en modo daemon, `0` en caso contrario.                                                                                                                                                                   |
| `console_log_level`          | Nivel de log para la salida de la consola. El valor predeterminado es `level`.                                                                                                                                                                                                                                                                      |
| `console_shutdown_log_level` | El nivel de apagado se usa para establecer el nivel de log de la consola durante el apagado del servidor.                                                                                                                                                                                                                                           |
| `console_startup_log_level`  | El nivel de inicio se usa para establecer el nivel de log de la consola durante el arranque del servidor. Después del arranque, el nivel de log vuelve a la configuración `console_log_level`                                                                                                                                                       |
| `count`                      | Política de rotación: número máximo de archivos de log históricos de ClickHouse que se conservan.                                                                                                                                                                                                                                                   |
| `errorlog`                   | La ruta al archivo de log de errores.                                                                                                                                                                                                                                                                                                               |
| `formatting.type`            | Formato de log para la salida de la consola. Actualmente, solo se admite `json`                                                                                                                                                                                                                                                                     |
| `level`                      | Nivel de log. Valores aceptables: `none` (desactiva el logging), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                                                                                                                                                                                          |
| `log`                        | La ruta al archivo de log.                                                                                                                                                                                                                                                                                                                          |
| `rotation`                   | Política de rotación: controla cuándo se rotan los archivos de log. La rotación puede basarse en el tamaño, el tiempo o una combinación de ambos. Ejemplos: 100M, daily, 100M,daily. Cuando el archivo de log supera el tamaño especificado o se alcanza el intervalo de tiempo indicado, se renombra y archiva, y se crea un nuevo archivo de log. |
| `shutdown_level`             | El nivel de apagado se usa para establecer el nivel del logger raíz durante el apagado del servidor.                                                                                                                                                                                                                                                |
| `size`                       | Política de rotación: tamaño máximo de los archivos de log en bytes. Cuando el tamaño del archivo de log supera este umbral, se renombra y archiva, y se crea un nuevo archivo de log.                                                                                                                                                              |
| `startup_level`              | El nivel de inicio se usa para establecer el nivel del logger raíz durante el arranque del servidor. Después del arranque, el nivel de log vuelve a la configuración `level`                                                                                                                                                                        |
| `stream_compress`            | Comprime los mensajes de log con LZ4. Establézcalo en `1` o `true` para activarlo.                                                                                                                                                                                                                                                                  |
| `syslog_level`               | Nivel de log para el registro en syslog.                                                                                                                                                                                                                                                                                                            |
| `use_syslog`                 | También reenvía la salida de log a syslog.                                                                                                                                                                                                                                                                                                          |

**Especificadores de formato del log**

Los nombres de archivo en las rutas `log` y `errorLog` admiten los siguientes especificadores de formato para el nombre de archivo resultante (la parte del directorio no los admite).

La columna &quot;Example&quot; muestra la salida en `2023-07-06 18:32:07`.

| Especificador | Descripción                                                                                                                                                                                     | Ejemplo                    |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%`          | % literal                                                                                                                                                                                       | `%`                        |
| `%n`          | Carácter de nueva línea                                                                                                                                                                         |                            |
| `%t`          | Carácter de tabulación horizontal                                                                                                                                                               |                            |
| `%Y`          | Año como número decimal, p. ej. 2017                                                                                                                                                            | `2023`                     |
| `%y`          | Últimos 2 dígitos del año como número decimal (rango [00,99])                                                                                                                                   | `23`                       |
| `%C`          | Primeros 2 dígitos del año como número decimal (rango [00,99])                                                                                                                                  | `20`                       |
| `%G`          | [Año ISO 8601 basado en semanas](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) de cuatro dígitos; es decir, el año que contiene la semana especificada. Normalmente, solo es útil con `%V` | `2023`                     |
| `%g`          | Últimos 2 dígitos del [año ISO 8601 basado en semanas](https://en.wikipedia.org/wiki/ISO_8601#Week_dates); es decir, el año que contiene la semana especificada.                                | `23`                       |
| `%b`          | Nombre abreviado del mes, p. ej. Oct (según la configuración regional)                                                                                                                          | `Jul`                      |
| `%h`          | Sinónimo de %b                                                                                                                                                                                  | `Jul`                      |
| `%B`          | Nombre completo del mes, p. ej. October (según la configuración regional)                                                                                                                       | `July`                     |
| `%m`          | Mes como número decimal (rango [01,12])                                                                                                                                                         | `07`                       |
| `%U`          | Semana del año como número decimal (el domingo es el primer día de la semana) (rango [00,53])                                                                                                   | `27`                       |
| `%W`          | Semana del año como número decimal (el lunes es el primer día de la semana) (rango [00,53])                                                                                                     | `27`                       |
| `%V`          | Número de semana ISO 8601 (rango [01,53])                                                                                                                                                       | `27`                       |
| `%j`          | Día del año como número decimal (rango [001,366])                                                                                                                                               | `187`                      |
| `%d`          | Día del mes como número decimal con relleno de ceros (rango [01,31]). Si tiene un solo dígito, va precedido de un cero.                                                                         | `06`                       |
| `%e`          | Día del mes como número decimal con relleno de espacios (rango [1,31]). Si tiene un solo dígito, va precedido de un espacio.                                                                    | `&nbsp; 6`                 |
| `%a`          | Nombre abreviado del día de la semana, p. ej. Fri (según la configuración regional)                                                                                                             | `Thu`                      |
| `%A`          | Nombre completo del día de la semana, p. ej. Friday (según la configuración regional)                                                                                                           | `Thursday`                 |
| `%w`          | Día de la semana como número entero, con domingo como 0 (rango [0-6])                                                                                                                           | `4`                        |
| `%u`          | Día de la semana como número decimal, donde el lunes es 1 (formato ISO 8601) (rango [1-7])                                                                                                      | `4`                        |
| `%H`          | Hora como número decimal, en formato de 24 horas (rango [00-23])                                                                                                                                | `18`                       |
| `%I`          | Hora como número decimal, en formato de 12 horas (rango [01,12])                                                                                                                                | `06`                       |
| `%M`          | Minuto como número decimal (rango [00,59])                                                                                                                                                      | `32`                       |
| `%S`          | Segundo como número decimal (rango [00,60])                                                                                                                                                     | `07`                       |
| `%c`          | Cadena estándar de fecha y hora, p. ej. Sun Oct 17 04:41:13 2010 (según la configuración regional)                                                                                              | `Thu Jul  6 18:32:07 2023` |
| `%x`          | Representación localizada de la fecha (según la configuración regional)                                                                                                                         | `07/06/23`                 |
| `%X`          | Representación localizada de la hora, p. ej. 18:40:20 o 6:40:20 PM (según la configuración regional)                                                                                            | `18:32:07`                 |
| `%D`          | Fecha corta MM/DD/YY, equivalente a %m/%d/%y                                                                                                                                                    | `07/06/23`                 |
| `%F`          | Fecha corta YYYY-MM-DD, equivalente a %Y-%m-%d                                                                                                                                                  | `2023-07-06`               |
| `%r`          | Hora local en formato de 12 horas (según la configuración regional)                                                                                                                             | `06:32:07 PM`              |
| `%R`          | Equivalente a &quot;%H:%M&quot;                                                                                                                                                                 | `18:32`                    |
| `%T`          | Equivalente a &quot;%H:%M:%S&quot; (el formato de hora ISO 8601)                                                                                                                                | `18:32:07`                 |
| `%p`          | Indicador local de a. m. o p. m. (según la configuración regional)                                                                                                                              | `PM`                       |
| `%z`          | Desplazamiento respecto a UTC en formato ISO 8601 (p. ej., -0430), o ningún carácter si la información de la zona horaria no está disponible                                                    | `+0800`                    |
| `%Z`          | Nombre o abreviatura de la zona horaria según la configuración regional, o ningún carácter si la información de la zona horaria no está disponible                                              | `Z AWST `                  |

**Ejemplo**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

Para imprimir los mensajes de log solo en la consola:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Sobrescrituras por nivel**

Se puede sobrescribir el nivel de log de loggers individuales. Por ejemplo, para silenciar todos los mensajes de los loggers &quot;Backup&quot; y &quot;RBAC&quot;.

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

Para escribir también mensajes de log en syslog:

```xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

Claves para `<syslog>`:

| Key        | Description                                                                                                                                                                                                                                                                                                            |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | La dirección de syslog en formato `host\[:port\]`. Si se omite, se usa el daemon local.                                                                                                                                                                                                                                |
| `hostname` | El nombre del host desde el que se envían los logs (opcional).                                                                                                                                                                                                                                                         |
| `facility` | La [palabra clave de facility](https://en.wikipedia.org/wiki/Syslog#Facility) de syslog. Debe especificarse en mayúsculas con el prefijo &quot;LOG&#95;&quot;, por ejemplo, `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, etc. Valor predeterminado: `LOG_USER` si se especifica `address`; en caso contrario, `LOG_DAEMON`. |
| `format`   | Formato del mensaje de log. Valores posibles: `bsd` y `syslog.`                                                                                                                                                                                                                                                        |

**Formatos de log**

Puede especificar el formato de log que se mostrará en el log de la consola. Actualmente, solo se admite JSON.

**Ejemplo**

A continuación se muestra un ejemplo de un log JSON de salida:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

Para habilitar el log en formato JSON, use el siguiente fragmento:

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**Cambiar el nombre de las claves de los logs JSON**

Los nombres de las claves pueden modificarse cambiando los valores de las etiquetas dentro de la etiqueta `<names>`. Por ejemplo, para cambiar `DATE_TIME` por `MY_DATE_TIME`, puedes usar `<date_time>MY_DATE_TIME</date_time>`.

**Omitir claves de los logs JSON**

Las propiedades del log pueden omitirse comentando la propiedad. Por ejemplo, si no quieres que tu log muestre `query_id`, puedes comentar la etiqueta `<query_id>`.

<div id="send_crash_reports">
  ## send_crash_reports
</div>

Configuración para el envío de informes de fallos al equipo de desarrolladores principales de ClickHouse.

Se agradece mucho habilitar esta opción, especialmente en entornos de preproducción.

Parámetros:

| Clave                 | Descripción                                                                                                                                                    |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`             | Indicador booleano para habilitar la función; `true` de forma predeterminada. Establécelo en `false` para evitar el envío de informes de fallos.               |
| `endpoint`            | Puedes sobrescribir la URL del endpoint para enviar informes de fallos.                                                                                        |
| `send_logical_errors` | `LOGICAL_ERROR` es como un `assert`; es un error en ClickHouse. Este indicador booleano habilita el envío de estas excepciones (valor predeterminado: `true`). |

**Uso recomendado**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

La parte pública de la clave del host se escribirá en el archivo known&#95;hosts
del lado del Client SSH en la primera conexión.

Las configuraciones de la clave del host están desactivadas de forma predeterminada.
Descomente las configuraciones de la clave del host y proporcione la ruta a la clave SSH correspondiente para activarlas:

Ejemplo:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

Puerto del servidor SSH que permite al usuario conectarse y ejecutar consultas de forma interactiva mediante el Client integrado a través del PTY.

Ejemplo:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

Permite configurar el almacenamiento en varios discos.

La configuración de almacenamiento tiene la siguiente estructura:

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

<div id="configuration-of-disks">
  ### Configuración de `disks`
</div>

La configuración de `disks` sigue la estructura que se indica a continuación:

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

Las subetiquetas anteriores definen la siguiente configuración de `disks`:

| Configuración           | Descripción                                                                                                   |
| ----------------------- | ------------------------------------------------------------------------------------------------------------- |
| `<disk_name_N>`         | El nombre del disco, que debe ser único.                                                                      |
| `path`                  | La ruta en la que se almacenarán los datos del servidor (directorios `data` y `shadow`). Debe terminar en `/` |
| `keep_free_space_bytes` | Tamaño del espacio libre reservado en el disco.                                                               |

:::note
El orden de los discos no importa.
:::

<div id="configuration-of-policies">
  ### Configuración de políticas
</div>

Las subetiquetas anteriores definen los siguientes ajustes para `policies`:

| Ajuste                       | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | Nombre de la política. Los nombres de las políticas deben ser únicos.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `volume_name_N`              | Nombre del volumen. Los nombres de los volúmenes deben ser únicos.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `disk`                       | El disco ubicado dentro del volumen.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `max_data_part_size_bytes`   | El tamaño máximo de un fragmento de datos que puede residir en cualquiera de los discos de este volumen. Si una fusión da como resultado un tamaño de fragmento que se espera que supere `max_data_part_size_bytes`, el fragmento se escribirá en el siguiente volumen. Básicamente, esta funcionalidad permite almacenar fragmentos nuevos o pequeños en un volumen rápido (SSD) y moverlos a un volumen frío (HDD) cuando alcanzan un tamaño grande. No use esta opción si la política solo tiene un volumen.                                                                                                     |
| `move_factor`                | La proporción del espacio libre disponible en el volumen. Si el espacio queda por debajo de ese valor, los datos comenzarán a transferirse al siguiente volumen, si existe. Para la transferencia, los fragmentos se ordenan por tamaño de mayor a menor (descendente) y se seleccionan los fragmentos cuyo tamaño total sea suficiente para cumplir la condición de `move_factor`; si el tamaño total de todos los fragmentos no es suficiente, se moverán todos los fragmentos.                                                                                                                                   |
| `perform_ttl_move_on_insert` | Desactiva el movimiento de datos con TTL vencido durante la inserción. De forma predeterminada (si está habilitado), si se inserta un fragmento de datos que ya ha vencido según la regla de movimiento por tiempo de vida, se mueve inmediatamente al volumen/disco especificado en la regla de movimiento. Esto puede ralentizar significativamente la inserción si el volumen/disco de destino es lento (por ejemplo, S3). Si está deshabilitado, la parte vencida de los datos se escribe en el volumen predeterminado y luego se mueve inmediatamente al volumen especificado en la regla para el TTL vencido. |
| `load_balancing`             | Política de equilibrio de discos: `round_robin` o `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `least_used_ttl_ms`          | Establece el tiempo de espera (en milisegundos) para actualizar el espacio disponible en todos los discos (`0`: actualizar siempre, `-1`: no actualizar nunca; el valor predeterminado es `60000`). Tenga en cuenta que, si el disco solo lo usa ClickHouse y no estará sujeto al redimensionamiento dinámico del sistema de archivos, puede usar el valor `-1`. En todos los demás casos, no se recomienda, ya que con el tiempo provocará una asignación incorrecta del espacio.                                                                                                                                  |
| `prefer_not_to_merge`        | Desactiva la fusión de partes de datos en este volumen. Nota: esto es potencialmente perjudicial y puede provocar ralentizaciones. Cuando este ajuste está habilitado (no lo haga), se prohíbe la fusión de datos en este volumen (lo cual es malo). Esto permite controlar cómo ClickHouse interactúa con discos lentos. Recomendamos no usarlo en absoluto.                                                                                                                                                                                                                                                       |
| `volume_priority`            | Define la prioridad (orden) en que se llenan los volúmenes. Cuanto menor sea el valor, mayor será la prioridad. Los valores del parámetro deben ser números naturales y cubrir el rango de 1 a N (N es el mayor valor de parámetro especificado) sin huecos.                                                                                                                                                                                                                                                                                                                                                        |

Para `volume_priority`:

* Si todos los volúmenes tienen este parámetro, se priorizan en el orden especificado.
* Si solo *algunos* volúmenes lo tienen, los volúmenes que no lo tienen tienen la prioridad más baja. Los que sí lo tienen se priorizan según el valor de la etiqueta; la prioridad del resto se determina por el orden en que aparecen en el archivo de configuración.
* Si *ningún* volumen tiene este parámetro, su orden se determina por el orden en que aparecen en el archivo de configuración.
* La prioridad de los volúmenes no puede ser la misma.

<div id="macros">
  ## macros
</div>

Sustituciones de parámetros para tablas replicadas.

Puede omitirse si no se utilizan tablas replicadas.

Para obtener más información, consulte la sección [Creación de tablas replicadas](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Ejemplo**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Nombre del grupo de réplicas para la base de datos Replicated.

El clúster creado por la base de datos Replicated estará formado por réplicas del mismo grupo.
Las consultas DDL solo esperarán a las réplicas del mismo grupo.

Vacío de forma predeterminada.

**Ejemplo**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

Tiempo máximo de espera de la sesión, en segundos.

Ejemplo:

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

Ajustes avanzados para tablas de [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Para más información, consulte el archivo de cabecera MergeTreeSettings.h.

**Ejemplo**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

Está desactivado de forma predeterminada.

**Activación**

Para activar manualmente la recopilación del historial de métricas [`system.metric_log`](../../operations/system-tables/metric_log.md), cree `/etc/clickhouse-server/config.d/metric_log.xml` con el contenido siguiente:

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**Desactivación**

Para desactivar la configuración `metric_log`, debe crear el siguiente archivo `/etc/clickhouse-server/config.d/disable_metric_log.xml` con el siguiente contenido:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

ajuste avanzado para las tablas de [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Esta configuración tiene mayor prioridad.

Para obtener más información, consulte el archivo de cabecera MergeTreeSettings.h.

**Ejemplo**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

Configuración de la tabla del sistema [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md).

<SystemLogParameters />

Ejemplo:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

<div id="openSSL">
  ## openSSL
</div>

Configuración SSL de Client y servidor.

La biblioteca `libpoco` proporciona soporte para SSL. Las opciones de configuración disponibles se explican en [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Los valores predeterminados se pueden consultar en [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Claves para la configuración de servidor/cliente:

| Opción                        | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Valor predeterminado                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | Habilita o deshabilita el almacenamiento en caché de sesiones. Debe usarse en combinación con `sessionIdContext`. Valores aceptables: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                               | `false`                                                                                    |
| `caConfig`                    | Ruta al archivo o directorio que contiene certificados de CA de confianza. Si apunta a un archivo, debe estar en formato PEM y puede contener varios certificados de CA. Si apunta a un directorio, debe contener un archivo .pem por cada certificado de CA. Los nombres de archivo se buscan mediante el valor hash del nombre de subject de la CA. Puede encontrar más detalles en la página man de [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                                                                            |
| `certificateFile`             | Ruta del archivo de certificado de cliente/servidor en formato PEM. Puede omitirse si `privateKeyFile` contiene el certificado.                                                                                                                                                                                                                                                                                                                                                                                                      |                                                                                            |
| `cipherList`                  | Cifrados de OpenSSL compatibles.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | Protocolos cuyo uso no está permitido.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |                                                                                            |
| `extendedVerification`        | Si está habilitada, verifica que el CN o SAN del certificado coincida con el nombre de host del par.                                                                                                                                                                                                                                                                                                                                                                                                                                 | `false`                                                                                    |
| `fips`                        | Activa el modo FIPS de OpenSSL. Compatible si la versión de OpenSSL de la biblioteca admite FIPS.                                                                                                                                                                                                                                                                                                                                                                                                                                    | `false`                                                                                    |
| `invalidCertificateHandler`   | Clase (una subclase de CertificateHandler) para la validación de certificados no válidos. Por ejemplo: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                                                                                                                                            | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | Si se utilizarán los certificados de CA integrados de OpenSSL. ClickHouse asume que los certificados de CA integrados se encuentran en el archivo `/etc/ssl/cert.pem` (resp. en el directorio `/etc/ssl/certs`) o en el archivo (resp. directorio) especificado por la variable de entorno `SSL_CERT_FILE` (resp. `SSL_CERT_DIR`).                                                                                                                                                                                                   | `true`                                                                                     |
| `preferServerCiphers`         | Cifrados de servidor preferidos por el cliente.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `false`                                                                                    |
| `privateKeyFile`              | Ruta al archivo con la clave privada del certificado PEM. El archivo puede contener una clave y un certificado al mismo tiempo.                                                                                                                                                                                                                                                                                                                                                                                                      |                                                                                            |
| `privateKeyPassphraseHandler` | Clase (subclase de PrivateKeyPassphraseHandler) que solicita la frase de contraseña necesaria para acceder a la clave privada. Por ejemplo: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                                                                        | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | Requiere una conexión TLSv1. Valores aceptables: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `false`                                                                                    |
| `requireTLSv1_1`              | Requiere una conexión TLSv1.1. Valores aceptables: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `false`                                                                                    |
| `requireTLSv1_2`              | Requiere una conexión TLSv1.2. Valores aceptables: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `false`                                                                                    |
| `sessionCacheSize`            | El número máximo de sesiones que el servidor mantiene en caché. Un valor de `0` significa que el número de sesiones es ilimitado.                                                                                                                                                                                                                                                                                                                                                                                                    | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | Un conjunto único de caracteres aleatorios que el servidor añade a cada identificador generado. La longitud de la cadena no debe superar `SSL_MAX_SSL_SESSION_ID_LENGTH`. Este parámetro siempre es recomendable, ya que ayuda a evitar problemas tanto si el servidor almacena en caché la sesión como si el cliente solicitó el almacenamiento en caché.                                                                                                                                                                           | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | Tiempo de almacenamiento en caché de la sesión en el servidor, en horas.                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `2`                                                                                        |
| `verificationDepth`           | La longitud máxima de la cadena de verificación. La verificación fallará si la longitud de la cadena de certificados supera el valor establecido.                                                                                                                                                                                                                                                                                                                                                                                    | `9`                                                                                        |
| `verificationMode`            | El método para verificar los certificados del nodo. Los detalles están en la descripción de la class [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h). Valores posibles: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                                                                              | `relaxed`                                                                                  |

**Ejemplo de configuración:**

```xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

<div id="part_log">
  ## part_log
</div>

Registra eventos asociados con [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), por ejemplo, al añadir o fusionar datos. Puede usar el log para simular algoritmos de fusión y comparar sus características. También puede visualizar el proceso de fusión.

Las consultas se registran en la tabla [system.part&#95;log](/es/operations/system-tables/part_log), no en un archivo independiente. Puede configurar el nombre de esta tabla mediante el parámetro `table` (consulte más abajo).

<SystemLogParameters />

**Ejemplo**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

<div id="processors_profile_log">
  ## processors_profile_log
</div>

Configuración de la tabla del sistema [`processors_profile_log`](../system-tables/processors_profile_log.md).

<SystemLogParameters />

La configuración predeterminada es la siguiente:

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

<div id="prometheus">
  ## prometheus
</div>

Exposición de datos de métricas para scraping con [Prometheus](https://prometheus.io).

Configuración:

* `endpoint` – endpoint HTTP para el scraping de métricas por parte del servidor Prometheus. Debe comenzar con &#39;/&#39;.
* `port` – Puerto de `endpoint`.
* `metrics` – Expone métricas de la tabla [system.metrics](/es/operations/system-tables/metrics).
* `events` – Expone métricas de la tabla [system.events](/es/operations/system-tables/events).
* `asynchronous_metrics` – Expone los valores actuales de las métricas de la tabla [system.asynchronous&#95;metrics](/es/operations/system-tables/asynchronous_metrics).
* `errors` - Expone el número de errores por códigos de error producidos desde el último reinicio del servidor. Esta información también puede obtenerse de la tabla [system.errors](/es/operations/system-tables/errors).

**Ejemplo**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

Compruebe (reemplace `127.0.0.1` por la dirección IP o el nombre de host de su servidor ClickHouse):

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

Configuración para registrar las consultas recibidas con la opción [log&#95;queries=1](../../operations/settings/settings.md).

Las consultas se registran en la tabla [system.query&#95;log](/es/operations/system-tables/query_log), no en un archivo independiente. Puede cambiar el nombre de la tabla mediante el parámetro `table` (consulte más abajo).

<SystemLogParameters />

Si la tabla no existe, ClickHouse la creará. Si la estructura del registro de consultas cambió al actualizar el servidor de ClickHouse, la tabla con la estructura anterior se renombra y se crea automáticamente una nueva tabla.

**Ejemplo**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

<div id="query_metric_log">
  ## query_metric_log
</div>

Está desactivado de forma predeterminada.

**Activación**

Para activar manualmente la recopilación del historial de métricas [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), cree `/etc/clickhouse-server/config.d/query_metric_log.xml` con el siguiente contenido:

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**Desactivar**

Para desactivar la configuración `query_metric_log`, debe crear el siguiente archivo `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` con el siguiente contenido:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

Configuración de la [caché de consultas](../query-cache.md).

Los siguientes ajustes están disponibles:

| Configuración             | Descripción                                                                                                 | Valor predeterminado |
| ------------------------- | ----------------------------------------------------------------------------------------------------------- | -------------------- |
| `max_entries`             | El número máximo de resultados de consultas `SELECT` almacenados en la caché.                               | `1024`               |
| `max_entry_size_in_bytes` | El tamaño máximo en bytes que pueden tener los resultados de consultas `SELECT` para guardarse en la caché. | `1048576`            |
| `max_entry_size_in_rows`  | El número máximo de filas que pueden tener los resultados de consultas `SELECT` para guardarse en la caché. | `30000000`           |
| `max_size_in_bytes`       | El tamaño máximo de la caché en bytes. `0` significa que la caché de consultas está deshabilitada.          | `1073741824`         |

:::note

* Los cambios en la configuración surten efecto de inmediato.
* Los datos de la caché de consultas se asignan en DRAM. Si la memoria es escasa, asegúrese de establecer un valor bajo para `max_size_in_bytes` o deshabilitar por completo la caché de consultas.
  :::

**Ejemplo**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

Configuración para registrar los hilos de las consultas recibidas con la opción [log&#95;query&#95;threads=1](/es/operations/settings/settings#log_query_threads).

Las consultas se registran en la tabla [system.query&#95;thread&#95;log](/es/operations/system-tables/query_thread_log), no en un archivo independiente. Puede cambiar el nombre de la tabla mediante el parámetro `table` (consulte más abajo).

<SystemLogParameters />

Si la tabla no existe, ClickHouse la creará. Si la estructura del registro de hilos de consultas cambió al actualizar el servidor ClickHouse, la tabla con la estructura anterior se renombra y se crea automáticamente una tabla nueva.

**Ejemplo**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

<div id="query_views_log">
  ## query_views_log
</div>

Configuración para registrar vistas (en vivo, materializadas, etc.) según las consultas recibidas con la configuración [log&#95;query&#95;views=1](/es/operations/settings/settings#log_query_views).

Las consultas se registran en la tabla [system.query&#95;views&#95;log](/es/operations/system-tables/query_views_log), no en un archivo aparte. Puede cambiar el nombre de la tabla en el parámetro `table` (consulte a continuación).

<SystemLogParameters />

Si la tabla no existe, ClickHouse la creará. Si la estructura del registro de vistas de consultas cambió al actualizar el servidor de ClickHouse, la tabla con la estructura anterior se renombra y se crea automáticamente una nueva.

**Ejemplo**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

<div id="text_log">
  ## text_log
</div>

Configuración de la [tabla del sistema text&#95;log](/es/operations/system-tables/text_log) para registrar mensajes de texto.

<SystemLogParameters />

Además:

| Configuración | Descripción                                                                                 | Valor predeterminado |
| ------------- | ------------------------------------------------------------------------------------------- | -------------------- |
| `level`       | Nivel máximo de mensaje (el valor predeterminado es `Trace`) que se almacenará en la tabla. | `Trace`              |

**Ejemplo**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

<div id="trace_log">
  ## trace_log
</div>

Configuración para el funcionamiento de la tabla del sistema [trace&#95;log](/es/operations/system-tables/trace_log).

<SystemLogParameters />

El archivo de configuración del servidor por defecto, `config.xml`, contiene la siguiente sección de configuración:

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

Configuración de la tabla del sistema [asynchronous&#95;insert&#95;log](/es/operations/system-tables/asynchronous_insert_log) para el registro de inserciones asíncronas.

<SystemLogParameters />

**Ejemplo**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

<div id="crash_log">
  ## crash_log
</div>

Configuración para la operación de la [tabla del sistema crash&#95;log](../../operations/system-tables/crash_log.md).

Las siguientes configuraciones pueden establecerse mediante subetiquetas:

| Configuración                      | Descripción                                                                                                                                                        | Predeterminado      | Nota                                                                                                                                          |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | Umbral para la cantidad de líneas. Si se alcanza el umbral, se inicia en segundo plano el volcado de los logs al disco.                                            | `max_size_rows / 2` |                                                                                                                                               |
| `database`                         | Nombre de la base de datos.                                                                                                                                        |                     |                                                                                                                                               |
| `engine`                           | [Definición del motor MergeTree](/es/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) para una tabla del sistema.            |                     | No puede usarse si se define `partition_by` u `order_by`. Si no se especifica, se selecciona `MergeTree` de forma predeterminada              |
| `flush_interval_milliseconds`      | Intervalo para volcar los datos del búfer en memoria a la tabla.                                                                                                   | `7500`              |                                                                                                                                               |
| `flush_on_crash`                   | Establece si los logs deben volcarse al disco en caso de fallo.                                                                                                    | `false`             |                                                                                                                                               |
| `max_size_rows`                    | Tamaño máximo en líneas para los logs. Cuando la cantidad de logs no volcados alcanza `max_size`, los logs se vuelcan al disco.                                    | `1024`              |                                                                                                                                               |
| `order_by`                         | [Clave de ordenación personalizada](/es/engines/table-engines/mergetree-family/mergetree#order_by) para una tabla del sistema. No puede usarse si se define `engine`. |                     | Si se especifica `engine` para la tabla del sistema, el parámetro `order_by` debe especificarse directamente dentro de &#39;engine&#39;       |
| `partition_by`                     | [Clave de partición personalizada](/es/engines/table-engines/mergetree-family/custom-partitioning-key.md) para una tabla del sistema.                                 |                     | Si se especifica `engine` para la tabla del sistema, el parámetro `partition_by` debe especificarse directamente dentro de &#39;engine&#39;   |
| `reserved_size_rows`               | Tamaño de memoria preasignado en líneas para los logs.                                                                                                             | `1024`              |                                                                                                                                               |
| `settings`                         | [Parámetros adicionales](/es/engines/table-engines/mergetree-family/mergetree/#settings) que controlan el comportamiento de MergeTree (opcional).                     |                     | Si se especifica `engine` para la tabla del sistema, el parámetro `settings` debe especificarse directamente dentro de &#39;engine&#39;       |
| `storage_policy`                   | Nombre de la política de almacenamiento que se usará para la tabla (opcional).                                                                                     |                     | Si se especifica `engine` para la tabla del sistema, el parámetro `storage_policy` debe especificarse directamente dentro de &#39;engine&#39; |
| `table`                            | Nombre de la tabla del sistema.                                                                                                                                    |                     |                                                                                                                                               |
| `ttl`                              | Especifica el [TTL](/es/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl) de la tabla.                                                     |                     | Si se especifica `engine` para la tabla del sistema, el parámetro `ttl` debe especificarse directamente dentro de &#39;engine&#39;            |

El archivo de configuración predeterminado del servidor, `config.xml`, contiene la siguiente sección de configuración:

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

Esta configuración especifica la ruta de caché para los discos en caché personalizados (creados desde SQL).
`custom_cached_disks_base_directory` tiene prioridad sobre `filesystem_caches_path` para los discos personalizados (se encuentra en `filesystem_caches_path.xml`),
que se usa si el primero no está presente.
La ruta de configuración de la caché del sistema de archivos debe estar dentro de ese directorio;
de lo contrario, se producirá una excepción que impedirá la creación del disco.

:::note
Esto no afectará a los discos creados en una versión anterior para los que se haya actualizado el servidor.
En este caso, no se producirá ninguna excepción, para permitir que el servidor se inicie correctamente.
:::

Ejemplo:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

Configuración de la tabla del sistema [backup&#95;log](../../operations/system-tables/backup_log.md) para registrar las operaciones `BACKUP` y `RESTORE`.

<SystemLogParameters />

**Ejemplo**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

<div id="blob_storage_log">
  ## blob_storage_log
</div>

Configuración de la tabla del sistema [`blob_storage_log`](../system-tables/blob_storage_log.md).

<SystemLogParameters />

Ejemplo:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

Reglas basadas en expresiones regulares que se aplican a las consultas, así como a todos los mensajes de registro antes de almacenarlos en los logs del servidor,
en las tablas [`system.query_log`](/es/operations/system-tables/query_log), [`system.text_log`](/es/operations/system-tables/text_log) y [`system.processes`](/es/operations/system-tables/processes), y en los logs enviados al client. Esto permite evitar
la filtración de datos sensibles de las consultas SQL, como nombres, correos electrónicos, identificadores personales o números de tarjetas de crédito, en los logs.

**Ejemplo**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**Campos de configuración**:

| Setting   | Description                                                                                     |
| --------- | ----------------------------------------------------------------------------------------------- |
| `name`    | nombre de la regla (opcional)                                                                   |
| `regexp`  | expresión regular compatible con RE2 (obligatorio)                                              |
| `replace` | cadena de sustitución para datos sensibles (opcional; de forma predeterminada, seis asteriscos) |

Las reglas de enmascaramiento se aplican a la consulta completa (para evitar fugas de datos sensibles en consultas malformadas o que no se pueden analizar).

La tabla [`system.events`](/es/operations/system-tables/events) incluye el contador `QueryMaskingRulesMatch`, que indica el número total de coincidencias con las reglas de enmascaramiento de consultas.

Para las consultas distribuidas, cada servidor debe configurarse por separado; de lo contrario, las subconsultas enviadas a otros
nodos se almacenarán sin enmascaramiento.

<div id="remote_servers">
  ## remote_servers
</div>

Configuración de los clústeres utilizada por el motor de tabla [Distributed](../../engines/table-engines/special/distributed.md) y por la función de tabla `cluster`.

**Ejemplo**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

Para el valor del atributo `incl`, consulte la sección &quot;[Archivos de configuración](/es/operations/configuration-files)&quot;.

**Véase también**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [Cluster Discovery](../../operations/cluster-discovery.md)
* [motor de base de datos Replicated](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

Lista de hosts que se permite usar en los motores de almacenamiento y las funciones de tabla relacionados con URL.

Al añadir un host con la etiqueta xml `\<host\>`:

* debe especificarse exactamente como aparece en la URL, ya que el nombre se comprueba antes de la resolución DNS. Por ejemplo: `<host>clickhouse.com</host>`
* si el puerto se especifica explícitamente en la URL, se comprueba `host:port` en su conjunto. Por ejemplo: `<host>clickhouse.com:80</host>`
* si el host se especifica sin puerto, se permite cualquier puerto de ese host. Por ejemplo: si se especifica `<host>clickhouse.com</host>`, se permiten `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS), etc.
* si el host se especifica como una dirección IP, se comprueba tal como aparece en la URL. Por ejemplo: `[2a02:6b8:a::a]`.
* si hay redirecciones y la compatibilidad con redirecciones está habilitada, se comprueba cada redirección (el campo `location`).

Por ejemplo:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## timezone
</div>

La zona horaria del servidor.

Se especifica como un identificador de IANA para la zona horaria UTC o una ubicación geográfica (por ejemplo, Africa/Abidjan).

La zona horaria es necesaria para las conversiones entre los formatos String y DateTime cuando los campos DateTime se muestran en formato de texto (en pantalla o en un archivo), y al obtener un valor DateTime a partir de una cadena. Además, la zona horaria se utiliza en las funciones que trabajan con la hora y la fecha si no recibieron la zona horaria en los parámetros de entrada.

**Ejemplo**

```xml
<timezone>Asia/Istanbul</timezone>
```

**Véase también**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

Puerto para la comunicación con cliente mediante el protocolo TCP.

**Ejemplo**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

Puerto TCP para la comunicación segura con cliente. Úselo con la configuración de [OpenSSL](#openssl).

**Valor predeterminado**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

Puerto para comunicarse con los client a través del protocolo MySQL.

:::note

* Los enteros positivos especifican el número de puerto en el que se escuchará
* Los valores vacíos se utilizan para deshabilitar la comunicación con los client a través del protocolo MySQL.
  :::

**Ejemplo**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

Puerto para comunicarse con cliente a través del protocolo PostgreSQL.

:::note

* Los enteros positivos especifican el número de puerto en el que se escuchará
* Los valores vacíos se usan para deshabilitar la comunicación con cliente a través del protocolo PostgreSQL.
  :::

**Ejemplo**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

Configuración para convertir prefijos de URL abreviados o simbólicos en URL completas.

Ejemplo:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

<div id="user_defined_path">
  ## user_defined_path
</div>

El directorio que contiene archivos definidos por el usuario. Se utiliza para las funciones definidas por el usuario en SQL [SQL User Defined Functions](/es/sql-reference/functions/udf).

**Ejemplo**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

Ruta al archivo que contiene:

* Configuraciones de usuarios.
* Permisos de acceso.
* Perfiles de configuración.
* Configuración de cuotas.

**Ejemplo**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

Configuración de mejoras opcionales en el sistema de control de acceso.

| Configuración                                   | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Predeterminado |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `on_cluster_queries_require_cluster_grant`      | Establece si las consultas `ON CLUSTER` requieren el privilegio `CLUSTER`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `true`         |
| `role_cache_expiration_time_seconds`            | Establece la cantidad de segundos desde el último acceso durante los cuales un rol se almacena en la caché de roles.                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `600`          |
| `select_from_information_schema_requires_grant` | Establece si `SELECT * FROM information_schema.<table>` requiere algún privilegio o puede ejecutarse por cualquier usuario. Si se establece en true, esta consulta requiere `GRANT SELECT ON information_schema.<table>`, igual que con las tablas normales.                                                                                                                                                                                                                                                                                                                     | `true`         |
| `select_from_system_db_requires_grant`          | Establece si `SELECT * FROM system.<table>` requiere algún privilegio o puede ejecutarse por cualquier usuario. Si se establece en true, esta consulta requiere `GRANT SELECT ON system.<table>`, igual que con las tablas que no son del sistema. Excepciones: algunas tablas del sistema (`tables`, `columns`, `databases` y algunas tablas constantes como `one`, `contributors`) siguen siendo accesibles para todos; y si se ha concedido un privilegio `SHOW` (por ejemplo, `SHOW USERS`), la tabla del sistema correspondiente (es decir, `system.users`) será accesible. | `true`         |
| `settings_constraints_replace_previous`         | Establece si una restricción de un perfil de configuración para una determinada configuración anulará los efectos de la restricción anterior (definida en otros perfiles) para esa configuración, incluidos los campos que no estén establecidos por la nueva restricción. También habilita el tipo de restricción `changeable_in_readonly`.                                                                                                                                                                                                                                     | `true`         |
| `table_engines_require_grant`                   | Establece si crear una tabla con un motor de tabla específico requiere un privilegio.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `false`        |
| `throw_on_unmatched_row_policies`               | Establece si al leer de una tabla se debe lanzar una excepción si la tabla tiene políticas de fila, pero ninguna de ellas corresponde al usuario actual                                                                                                                                                                                                                                                                                                                                                                                                                          | `false`        |
| `users_without_row_policies_can_read_rows`      | Establece si los usuarios sin políticas de fila permisivas aún pueden leer filas mediante una consulta `SELECT`. Por ejemplo, si hay dos usuarios A y B y se define una política de fila solo para A, entonces, si esta configuración es true, el usuario B verá todas las filas. Si esta configuración es false, el usuario B no verá ninguna fila.                                                                                                                                                                                                                             | `true`         |

Ejemplo:

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

<div id="s3queue_log">
  ## s3queue_log
</div>

Configuración de la tabla del sistema `s3queue_log`.

<SystemLogParameters />

Los ajustes predeterminados son:

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

Configuración de la tabla del sistema &#39;dead&#95;letter&#95;queue&#39;.

<SystemLogParameters />

La configuración predeterminada es la siguiente:

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

Contiene ajustes que permiten a ClickHouse interactuar con un clúster de [ZooKeeper](http://zookeeper.apache.org/). ClickHouse usa ZooKeeper para almacenar los metadatos de las réplicas cuando se usan tablas replicadas. Si no se usan tablas replicadas, esta sección de parámetros puede omitirse.

Los siguientes ajustes pueden configurarse mediante subetiquetas:

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `node`                                          | Endpoint de ZooKeeper. Puede configurar varios endpoints. P. ej., `<node index="1"><host>example_host</host><port>2181</port></node>`. El atributo `index` especifica el orden de los nodos al intentar conectarse al clúster de ZooKeeper.                                                                                                                                                                              |
| `operation_timeout_ms`                          | Tiempo de espera máximo de una operación, en milisegundos.                                                                                                                                                                                                                                                                                                                                                               |
| `session_timeout_ms`                            | Tiempo de espera máximo de la sesión del client, en milisegundos.                                                                                                                                                                                                                                                                                                                                                        |
| `root` (optional)                               | El znode que se usa como raíz para los znodes utilizados por el servidor ClickHouse.                                                                                                                                                                                                                                                                                                                                     |
| `fallback_session_lifetime.min` (optional)      | Límite mínimo para la duración de una sesión de ZooKeeper hacia el nodo de fallback cuando el primario no está disponible (balanceo de carga). Se establece en segundos. Valor predeterminado: 3 horas.                                                                                                                                                                                                                  |
| `fallback_session_lifetime.max` (optional)      | Límite máximo para la duración de una sesión de ZooKeeper hacia el nodo de fallback cuando el primario no está disponible (balanceo de carga). Se establece en segundos. Valor predeterminado: 6 horas.                                                                                                                                                                                                                  |
| `identity` (optional)                           | Usuario y contraseña requeridos por ZooKeeper para acceder a los znodes solicitados.                                                                                                                                                                                                                                                                                                                                     |
| `use_compression` (optional)                    | Habilita la compresión en el protocolo Keeper si se establece en `true`.                                                                                                                                                                                                                                                                                                                                                 |
| `use_xid_64` (optional)                         | Habilita ID de transacción de 64 bits. Establézcalo en `true` para habilitar el formato extendido de ID de transacción. Valor predeterminado: `false`.                                                                                                                                                                                                                                                                   |
| `pass_opentelemetry_tracing_context` (optional) | Habilita la propagación del contexto de tracing de OpenTelemetry a las solicitudes de Keeper. Cuando está habilitado, se crearán spans de tracing para las operaciones de Keeper, lo que permite el tracing distribuido entre ClickHouse y Keeper. Consulte [Tracing ClickHouse Keeper Requests](/es/operations/opentelemetry#tracing-clickhouse-keeper-requests) para obtener más detalles. Valor predeterminado: `false`. |

También existe el ajuste `zookeeper_load_balancing` (opcional), que le permite seleccionar el algoritmo de selección de nodos de ZooKeeper:

| Algorithm Name                   | Description                                                                                                                                           |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `random`                         | selecciona aleatoriamente uno de los nodos de ZooKeeper.                                                                                              |
| `in_order`                       | selecciona el primer nodo de ZooKeeper; si no está disponible, selecciona el segundo, y así sucesivamente.                                            |
| `nearest_hostname`               | selecciona un nodo de ZooKeeper cuyo hostname sea lo más parecido posible al hostname del servidor; el hostname se compara con el prefijo del nombre. |
| `hostname_levenshtein_distance`  | igual que nearest&#95;hostname, pero compara el hostname mediante la distancia de Levenshtein.                                                        |
| `hostname_longest_common_prefix` | igual que nearest&#95;hostname, pero prefiere el nodo cuyo hostname comparte el prefijo común más largo con el hostname del servidor.                 |
| `hostname_longest_common_suffix` | igual que nearest&#95;hostname, pero prefiere el nodo cuyo hostname comparte el sufijo común más largo con el hostname del servidor.                  |
| `first_or_random`                | selecciona el primer nodo de ZooKeeper; si no está disponible, selecciona aleatoriamente uno de los nodos de ZooKeeper restantes.                     |
| `round_robin`                    | selecciona el primer nodo de ZooKeeper; si se produce una reconexión, selecciona el siguiente.                                                        |

**Configuración de ejemplo**

```xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**Véase también**

* [Replicación](../../engines/table-engines/mergetree-family/replication.md)
* [Guía del programador de ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [Comunicación segura opcional entre ClickHouse y ZooKeeper](/es/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

Método de almacenamiento de las cabeceras de las partes de datos en ZooKeeper. Esta configuración solo se aplica a la familia [`MergeTree`](/es/engines/table-engines/mergetree-family). Se puede especificar:

**Globalmente en la sección [merge&#95;tree](#merge_tree) del archivo `config.xml`**

ClickHouse usa esta configuración para todas las tablas del servidor. Puede cambiarla en cualquier momento. Las tablas existentes cambian su comportamiento cuando cambia esta configuración.

**Para cada tabla**

Al crear una tabla, especifique la correspondiente [configuración del motor](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). El comportamiento de una tabla existente con esta configuración no cambia, incluso si cambia la configuración global.

**Posibles valores**

* `0` — La funcionalidad está desactivada.
* `1` — La funcionalidad está activada.

Si [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper), las tablas [replicadas](../../engines/table-engines/mergetree-family/replication.md) almacenan de forma compacta las cabeceras de las partes de datos usando un único `znode`. Si la tabla contiene muchas columnas, este método de almacenamiento reduce significativamente el volumen de datos almacenados en ZooKeeper.

:::note
Después de aplicar `use_minimalistic_part_header_in_zookeeper = 1`, no podrá volver a una versión anterior del ClickHouse server que no admita esta configuración. Tenga cuidado al actualizar ClickHouse en los servidores de un clúster. No actualice todos los servidores a la vez. Es más seguro probar las nuevas versiones de ClickHouse en un entorno de prueba o solo en unos pocos servidores del clúster.

Las cabeceras de las partes de datos que ya se hayan almacenado con esta configuración no pueden restaurarse a su representación anterior (no compacta).
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

Gestiona la ejecución de [consultas DDL distribuidas](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) en el cluster.
Funciona solo si [ZooKeeper](/es/operations/server-configuration-parameters/settings#zookeeper) está habilitado.

Los ajustes configurables dentro de `<distributed_ddl>` incluyen:

| Ajuste                 | Descripción                                                                                                                                        | Valor predeterminado                        |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| `cleanup_delay_period` | la limpieza comienza después de recibir un evento de un nodo nuevo si la última limpieza se realizó hace al menos `cleanup_delay_period` segundos. | `60` segundos                               |
| `max_tasks_in_queue`   | el número máximo de tareas que puede haber en la cola.                                                                                             | `1,000`                                     |
| `path`                 | la ruta en Keeper para la `task_queue` de las consultas DDL                                                                                        |                                             |
| `pool_size`            | cuántas consultas `ON CLUSTER` pueden ejecutarse simultáneamente                                                                                   |                                             |
| `profile`              | el perfil utilizado para ejecutar las consultas DDL                                                                                                |                                             |
| `task_max_lifetime`    | elimina el nodo si su antigüedad supera este valor.                                                                                                | `7 * 24 * 60 * 60` (una semana en segundos) |

**Ejemplo**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

<div id="access_control_path">
  ## access_control_path
</div>

Ruta de la carpeta donde un servidor de ClickHouse almacena las configuraciones de usuarios y roles creadas mediante comandos SQL.

**Véase también**

* [Control de acceso y gestión de cuentas](/es/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

Determina si se permiten o no los tipos de contraseña en texto sin cifrar (inseguros).

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

Indica si se permite o no el tipo de contraseña insegura no&#95;password.

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

Prohíbe crear un usuario sin contraseña, a menos que se especifique explícitamente &#39;IDENTIFIED WITH no&#95;password&#39;.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

Tiempo de espera de sesión predeterminado, en segundos.

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

Establece el tipo de contraseña que se asignará automáticamente en consultas como `CREATE USER u IDENTIFIED BY 'p'`.

Los valores aceptados son:

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

Sección del archivo de configuración que contiene ajustes:

* Ruta al archivo de configuración con usuarios predefinidos.
* Ruta a la carpeta donde se almacenan los usuarios creados mediante comandos SQL.
* Ruta del nodo de ZooKeeper donde se almacenan y replican los usuarios creados mediante comandos SQL.

Si se especifica esta sección, no se usarán las rutas de [users&#95;config](/es/operations/server-configuration-parameters/settings#users_config) ni de [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path).

La sección `user_directories` puede contener cualquier cantidad de elementos; el orden de los elementos determina su precedencia (cuanto más arriba esté el elemento, mayor será la precedencia).

**Ejemplos**

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Los usuarios, los roles, las políticas por fila, las cuotas y los perfiles también se pueden almacenar en ZooKeeper:

```xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

También se pueden definir secciones `memory` — que significa almacenar información solo en memoria, sin escribirla en disco— y `ldap` — que significa almacenar información en un servidor LDAP.

Para añadir un servidor LDAP como directorio remoto de usuarios no definidos localmente, defina una única sección `ldap` con la siguiente configuración:

| Configuración | Descripción                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `roles`       | Sección con una lista de roles definidos localmente que se asignarán a cada usuario obtenido del servidor LDAP. Si no se especifican roles, el usuario no podrá realizar ninguna acción después de la autenticación. Si alguno de los roles indicados no está definido localmente en el momento de la autenticación, el intento de autenticación fallará como si la contraseña proporcionada fuera incorrecta. |
| `server`      | Uno de los nombres de servidor LDAP definidos en la sección de configuración `ldap_servers`. Este parámetro es obligatorio y no puede estar vacío.                                                                                                                                                                                                                                                             |

**Ejemplo**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

Define una lista de dominios de nivel superior personalizados que se añadirán, donde cada entrada tiene el formato `<name>/path/to/file</name>`.

Por ejemplo:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

Véase también:

* la función [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) y sus variaciones,
  que acepta el nombre de una lista de TLD personalizada y devuelve la parte del dominio que incluye los subdominios de nivel superior hasta el primer subdominio significativo.

<div id="proxy">
  ## proxy
</div>

Defina servidores proxy para las solicitudes HTTP y HTTPS, compatibles actualmente con almacenamiento S3, funciones de tabla S3 y funciones URL.

Hay tres formas de definir servidores proxy:

* variables de entorno
* listas de proxy
* resolvedores remotos de proxy.

También se admite omitir los servidores proxy para hosts específicos mediante `no_proxy`.

**Variables de entorno**

Las variables de entorno `http_proxy` y `https_proxy` le permiten especificar un
servidor proxy para un protocolo determinado. Si están configuradas en su sistema, deberían funcionar sin problemas.

Este es el enfoque más sencillo si un protocolo determinado tiene
un solo servidor proxy y ese servidor proxy no cambia.

**Listas de proxy**

Este enfoque le permite especificar uno o varios
servidores proxy para un protocolo. Si se define más de un servidor proxy,
ClickHouse usa los distintos proxies con un método round-robin, equilibrando la
carga entre los servidores. Este es el enfoque más sencillo si hay más de
un servidor proxy para un protocolo y la lista de servidores proxy no cambia.

**Plantilla de configuración**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

Seleccione un campo principal en las pestañas de abajo para ver sus campos secundarios:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Campo     | Descripción                          |
    | --------- | ------------------------------------ |
    | `<http>`  | Una lista de uno o más proxies HTTP  |
    | `<https>` | Una lista de uno o más proxies HTTPS |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | Campo   | Descripción      |
    | ------- | ---------------- |
    | `<uri>` | La URI del proxy |
  </TabItem>
</Tabs>

**Resolvers remotos de proxy**

Es posible que los servidores proxy cambien dinámicamente. En ese
caso, puede definir el endpoint de un resolver. ClickHouse envía
una solicitud GET vacía a ese endpoint; el resolver remoto debe devolver el host del proxy.
ClickHouse lo usará para formar la URI del proxy con la siguiente plantilla: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**Plantilla de configuración**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

Seleccione un campo principal en las pestañas de abajo para ver sus campos hijo:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | Campo     | Descripción                           |
    | --------- | ------------------------------------- |
    | `<http>`  | Una lista de uno o más resolvers* |
    | `<https>` | Una lista de uno o más resolvers* |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | Campo        | Descripción                                 |
    | ------------ | ------------------------------------------- |
    | `<resolver>` | El endpoint y otros detalles de un resolver |

    :::note
    Puede haber varios elementos `<resolver>`, pero solo se usa el primero
    `<resolver>` para un protocolo determinado. Cualquier otro elemento `<resolver>`
    para ese protocolo se ignora. Esto significa que el balanceo de carga
    (si es necesario) debe implementarse en el resolver remoto.
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | Campo                | Descripción                                                                                                                                                                                                  |
    | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
    | `<endpoint>`         | El URI del resolver de proxy                                                                                                                                                                                 |
    | `<proxy_scheme>`     | El protocolo del URI final del proxy. Puede ser `http` o `https`.                                                                                                                                            |
    | `<proxy_port>`       | El número de puerto del resolver de proxy                                                                                                                                                                    |
    | `<proxy_cache_time>` | El tiempo, en segundos, durante el cual ClickHouse debe almacenar en caché los valores del resolver. Establecer este valor en `0` hace que ClickHouse contacte al resolver para cada solicitud HTTP o HTTPS. |
  </TabItem>
</Tabs>

**Precedencia**

La configuración del proxy se determina en el siguiente orden:

| Orden | Configuración              |
| ----- | -------------------------- |
| 1.    | Resolvers remotos de proxy |
| 2.    | Listas de proxy            |
| 3.    | Variables de entorno       |

ClickHouse comprobará el tipo de resolver de mayor prioridad para el protocolo de la solicitud. Si no está definido,
comprobará el siguiente tipo de resolver con mayor prioridad, hasta llegar al resolver de entorno.
Esto también permite usar una combinación de tipos de resolver.

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

De forma predeterminada, se utiliza tunelización (es decir, `HTTP CONNECT`) para realizar solicitudes `HTTPS` a través de un proxy `HTTP`. Esta configuración puede usarse para deshabilitarla.

**no&#95;proxy**

De forma predeterminada, todas las solicitudes pasarán por el proxy. Para deshabilitarlo para hosts específicos, se debe establecer la variable `no_proxy`.
Puede establecerse dentro de la cláusula `<proxy>` para los resolvers de lista y remotos, y como variable de entorno para el resolver de entorno.
Admite direcciones IP, dominios, subdominios y el comodín `'*'` para omitirlo por completo. Los puntos iniciales se eliminan, igual que en curl.

**Ejemplo**

La siguiente configuración omite el proxy para las solicitudes a `clickhouse.cloud` y a todos sus subdominios (p. ej., `auth.clickhouse.cloud`).
Lo mismo se aplica a GitLab, aunque tenga un punto inicial. Tanto `gitlab.com` como `about.gitlab.com` omitirían el proxy.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

<div id="workload_path">
  ## workload_path
</div>

El directorio que se utiliza para almacenar todas las consultas `CREATE WORKLOAD` y `CREATE RESOURCE`. De forma predeterminada, se usa la carpeta `/workload/` dentro del directorio de trabajo del servidor.

**Ejemplo**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**Véase también**

* [Jerarquía de cargas de trabajo](/es/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

La ruta a un nodo de ZooKeeper, que se usa como almacenamiento para todas las consultas `CREATE WORKLOAD` y `CREATE RESOURCE`. Para garantizar la coherencia, todas las definiciones SQL se almacenan como valor de este único znode. De forma predeterminada, ZooKeeper no se utiliza y las definiciones se almacenan en [disco](#workload_path).

**Ejemplo**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**Véase también**

* [Jerarquía de cargas de trabajo](/es/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

Configuración de la [tabla del sistema `zookeeper_log`](/es/operations/system-tables/zookeeper_log).

La siguiente configuración se puede definir mediante subetiquetas:

<SystemLogParameters />

**Ejemplo**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```