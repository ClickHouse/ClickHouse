---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "Configuraci\xF3n del servidor"
---

# Configuración del servidor {#server-settings}

## builtin\_dictionaries\_reload\_interval {#builtin-dictionaries-reload-interval}

El intervalo en segundos antes de volver a cargar los diccionarios integrados.

ClickHouse recarga los diccionarios incorporados cada x segundos. Esto hace posible editar diccionarios “on the fly” sin reiniciar el servidor.

Valor predeterminado: 3600.

**Ejemplo**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compresión {#server-settings-compression}

Ajustes de compresión de datos para [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md)-mesas de motor.

!!! warning "Advertencia"
    No lo use si acaba de comenzar a usar ClickHouse.

Plantilla de configuración:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
    </case>
    ...
</compression>
```

`<case>` campo:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` o `zstd`.

Puede configurar múltiples `<case>` apartado.

Acciones cuando se cumplen las condiciones:

-   Si un elemento de datos coincide con un conjunto de condiciones, ClickHouse utiliza el método de compresión especificado.
-   Si un elemento de datos coincide con varios conjuntos de condiciones, ClickHouse utiliza el primer conjunto de condiciones coincidente.

Si no se cumplen condiciones para un elemento de datos, ClickHouse utiliza el `lz4` compresión.

**Ejemplo**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default\_database {#default-database}

La base de datos predeterminada.

Para obtener una lista de bases de datos, [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) consulta.

**Ejemplo**

``` xml
<default_database>default</default_database>
```

## default\_profile {#default-profile}

Perfil de configuración predeterminado.

Los perfiles de configuración se encuentran en el archivo especificado en el parámetro `user_config`.

**Ejemplo**

``` xml
<default_profile>default</default_profile>
```

## Diccionarios\_config {#server_configuration_parameters-dictionaries_config}

La ruta de acceso al archivo de configuración para diccionarios externos.

Camino:

-   Especifique la ruta absoluta o la ruta relativa al archivo de configuración del servidor.
-   La ruta puede contener comodines \* y ?.

Ver también “[Diccionarios externos](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**Ejemplo**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## Diccionarios\_lazy\_load {#server_configuration_parameters-dictionaries_lazy_load}

La carga perezosa de los diccionarios.

Si `true`, entonces cada diccionario es creado en el primer uso. Si se produce un error en la creación del diccionario, la función que estaba utilizando el diccionario produce una excepción.

Si `false`, todos los diccionarios se crean cuando se inicia el servidor, y si hay un error, el servidor se apaga.

El valor predeterminado es `true`.

**Ejemplo**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format\_schema\_path {#server_configuration_parameters-format_schema_path}

La ruta de acceso al directorio con los esquemas para los datos de entrada, como los esquemas [CapnProto](../../interfaces/formats.md#capnproto) formato.

**Ejemplo**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## grafito {#server_configuration_parameters-graphite}

Envío de datos a [Grafito](https://github.com/graphite-project).

Configuración:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root\_path – Prefix for keys.
-   metrics – Sending data from the [sistema.métricas](../../operations/system-tables.md#system_tables-metrics) tabla.
-   events – Sending deltas data accumulated for the time period from the [sistema.evento](../../operations/system-tables.md#system_tables-events) tabla.
-   events\_cumulative – Sending cumulative data from the [sistema.evento](../../operations/system-tables.md#system_tables-events) tabla.
-   asynchronous\_metrics – Sending data from the [sistema.asynchronous\_metrics](../../operations/system-tables.md#system_tables-asynchronous_metrics) tabla.

Puede configurar múltiples `<graphite>` clausula. Por ejemplo, puede usar esto para enviar datos diferentes a intervalos diferentes.

**Ejemplo**

``` xml
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

## graphite\_rollup {#server_configuration_parameters-graphite-rollup}

Ajustes para reducir los datos de grafito.

Para obtener más información, consulte [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Ejemplo**

``` xml
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

## http\_port/https\_port {#http-porthttps-port}

El puerto para conectarse al servidor a través de HTTP(s).

Si `https_port` se especifica, [openSSL](#server_configuration_parameters-openssl) debe ser configurado.

Si `http_port` se especifica, la configuración de OpenSSL se ignora incluso si está establecida.

**Ejemplo**

``` xml
<https_port>9999</https_port>
```

## http\_server\_default\_response {#server_configuration_parameters-http_server_default_response}

La página que se muestra de forma predeterminada al acceder al servidor HTTP de ClickHouse.
El valor predeterminado es “Ok.” (con un avance de línea al final)

**Ejemplo**

Abrir `https://tabix.io/` al acceder `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include\_from {#server_configuration_parameters-include_from}

La ruta al archivo con sustituciones.

Para obtener más información, consulte la sección “[Archivos de configuración](../configuration-files.md#configuration_files)”.

**Ejemplo**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## Interesante {#interserver-http-port}

Puerto para el intercambio de datos entre servidores ClickHouse.

**Ejemplo**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## Sistema abierto {#interserver-http-host}

El nombre de host que pueden utilizar otros servidores para acceder a este servidor.

Si se omite, se define de la misma manera que el `hostname-f` comando.

Útil para separarse de una interfaz de red específica.

**Ejemplo**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver\_http\_credentials {#server-settings-interserver-http-credentials}

El nombre de usuario y la contraseña utilizados para [replicación](../../engines/table-engines/mergetree-family/replication.md) con los motores Replicated\*. Estas credenciales sólo se utilizan para la comunicación entre réplicas y no están relacionadas con las credenciales de los clientes de ClickHouse. El servidor está comprobando estas credenciales para conectar réplicas y utiliza las mismas credenciales cuando se conecta a otras réplicas. Por lo tanto, estas credenciales deben establecerse igual para todas las réplicas de un clúster.
De forma predeterminada, la autenticación no se utiliza.

Esta sección contiene los siguientes parámetros:

-   `user` — username.
-   `password` — password.

**Ejemplo**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep\_alive\_timeout {#keep-alive-timeout}

El número de segundos que ClickHouse espera las solicitudes entrantes antes de cerrar la conexión. El valor predeterminado es de 3 segundos.

**Ejemplo**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen\_host {#server_configuration_parameters-listen_host}

Restricción en hosts de los que pueden provenir las solicitudes. Si desea que el servidor responda a todos ellos, especifique `::`.

Ejemplos:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## registrador {#server_configuration_parameters-logger}

Configuración de registro.

Claves:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`y`errorlog`. Una vez que el archivo alcanza `size`, ClickHouse archiva y cambia el nombre, y crea un nuevo archivo de registro en su lugar.
-   count – The number of archived log files that ClickHouse stores.

**Ejemplo**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

También se admite la escritura en el syslog. Config ejemplo:

``` xml
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

Claves:

-   use\_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [La palabra clave syslog facility](https://en.wikipedia.org/wiki/Syslog#Facility) en letras mayúsculas con el “LOG\_” prefijo: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` y así sucesivamente).
    Valor predeterminado: `LOG_USER` si `address` se especifica, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` y `syslog.`

## macro {#macros}

Sustituciones de parámetros para tablas replicadas.

Se puede omitir si no se utilizan tablas replicadas.

Para obtener más información, consulte la sección “[Creación de tablas replicadas](../../engines/table-engines/mergetree-family/replication.md)”.

**Ejemplo**

``` xml
<macros incl="macros" optional="true" />
```

## Método de codificación de datos: {#server-mark-cache-size}

Tamaño aproximado (en bytes) de la memoria caché de marcas utilizadas por los motores de [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md) familia.

La memoria caché se comparte para el servidor y la memoria se asigna según sea necesario. El tamaño de la memoria caché debe ser al menos 5368709120.

**Ejemplo**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max\_concurrent\_queries {#max-concurrent-queries}

El número máximo de solicitudes procesadas simultáneamente.

**Ejemplo**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max\_connections {#max-connections}

El número máximo de conexiones entrantes.

**Ejemplo**

``` xml
<max_connections>4096</max_connections>
```

## max\_open\_files {#max-open-files}

El número máximo de archivos abiertos.

Predeterminada: `maximum`.

Recomendamos usar esta opción en Mac OS X desde el `getrlimit()` función devuelve un valor incorrecto.

**Ejemplo**

``` xml
<max_open_files>262144</max_open_files>
```

## max\_table\_size\_to\_drop {#max-table-size-to-drop}

Restricción en la eliminación de tablas.

Si el tamaño de un [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md) mesa excede `max_table_size_to_drop` (en bytes), no puede eliminarlo usando una consulta DROP.

Si aún necesita eliminar la tabla sin reiniciar el servidor ClickHouse, cree el `<clickhouse-path>/flags/force_drop_table` y ejecute la consulta DROP.

Valor predeterminado: 50 GB.

El valor 0 significa que puede eliminar todas las tablas sin restricciones.

**Ejemplo**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge\_tree {#server_configuration_parameters-merge_tree}

Ajuste fino para tablas en el [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md).

Para obtener más información, vea MergeTreeSettings.h archivo de encabezado.

**Ejemplo**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

Configuración cliente/servidor SSL.

El soporte para SSL es proporcionado por el `libpoco` biblioteca. La interfaz se describe en el archivo [Nombre de la red inalámbrica (SSID):h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Claves para la configuración del servidor/cliente:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contiene el certificado.
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node's certificates. Details are in the description of the [Contexto](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) clase. Valores posibles: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Valores aceptables: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. Este parámetro siempre se recomienda ya que ayuda a evitar problemas tanto si el servidor almacena en caché la sesión como si el cliente solicita el almacenamiento en caché. Valor predeterminado: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1\_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**Ejemplo de configuración:**

``` xml
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

## part\_log {#server_configuration_parameters-part-log}

Registro de eventos asociados con [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md). Por ejemplo, agregar o fusionar datos. Puede utilizar el registro para simular algoritmos de combinación y comparar sus características. Puede visualizar el proceso de fusión.

Las consultas se registran en el [sistema.part\_log](../../operations/system-tables.md#system_tables-part-log) tabla, no en un archivo separado. Puede configurar el nombre de esta tabla en el `table` parámetro (ver más abajo).

Utilice los siguientes parámetros para configurar el registro:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [clave de partición personalizada](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**Ejemplo**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## camino {#server_configuration_parameters-path}

La ruta de acceso al directorio que contiene los datos.

!!! note "Nota"
    La barra diagonal es obligatoria.

**Ejemplo**

``` xml
<path>/var/lib/clickhouse/</path>
```

## prometeo {#server_configuration_parameters-prometheus}

Exponer datos de métricas para raspar desde [Prometeo](https://prometheus.io).

Configuración:

-   `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from ‘/’.
-   `port` – Port for `endpoint`.
-   `metrics` – Flag that sets to expose metrics from the [sistema.métricas](../system-tables.md#system_tables-metrics) tabla.
-   `events` – Flag that sets to expose metrics from the [sistema.evento](../system-tables.md#system_tables-events) tabla.
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [sistema.asynchronous\_metrics](../system-tables.md#system_tables-asynchronous_metrics) tabla.

**Ejemplo**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## query\_log {#server_configuration_parameters-query-log}

Configuración de las consultas de registro recibidas con [log\_queries=1](../settings/settings.md) configuración.

Las consultas se registran en el [sistema.query\_log](../../operations/system-tables.md#system_tables-query_log) tabla, no en un archivo separado. Puede cambiar el nombre de la tabla en el `table` parámetro (ver más abajo).

Utilice los siguientes parámetros para configurar el registro:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [clave de partición personalizada](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) para una mesa.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

Si la tabla no existe, ClickHouse la creará. Si la estructura del registro de consultas cambió cuando se actualizó el servidor ClickHouse, se cambia el nombre de la tabla con la estructura anterior y se crea una nueva tabla automáticamente.

**Ejemplo**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## Sistema abierto {#server_configuration_parameters-query-thread-log}

Configuración de subprocesos de registro de consultas recibidas con [Log\_query\_threads = 1](../settings/settings.md#settings-log-query-threads) configuración.

Las consultas se registran en el [sistema.Sistema abierto.](../../operations/system-tables.md#system_tables-query-thread-log) tabla, no en un archivo separado. Puede cambiar el nombre de la tabla en el `table` parámetro (ver más abajo).

Utilice los siguientes parámetros para configurar el registro:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [clave de partición personalizada](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) para una tabla del sistema.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

Si la tabla no existe, ClickHouse la creará. Si la estructura del registro de subprocesos de consulta cambió cuando se actualizó el servidor ClickHouse, se cambia el nombre de la tabla con la estructura anterior y se crea una nueva tabla automáticamente.

**Ejemplo**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace\_log {#server_configuration_parameters-trace_log}

Ajustes para el [trace\_log](../../operations/system-tables.md#system_tables-trace_log) operación de la tabla del sistema.

Parámetros:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [Clave de partición personalizada](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) para una tabla del sistema.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

El archivo de configuración del servidor predeterminado `config.xml` contiene la siguiente sección de configuración:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query\_masking\_rules {#query-masking-rules}

Reglas basadas en Regexp, que se aplicarán a las consultas, así como a todos los mensajes de registro antes de almacenarlos en los registros del servidor,
`system.query_log`, `system.text_log`, `system.processes` tabla, y en los registros enviados al cliente. Eso permite prevenir
fuga de datos sensible de consultas SQL (como nombres, correos electrónicos,
identificadores o números de tarjetas de crédito) a los registros.

**Ejemplo**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

Campos de configuración:
- `name` - nombre de la regla (opcional)
- `regexp` - Expresión regular compatible con RE2 (obligatoria)
- `replace` - cadena de sustitución para datos confidenciales (opcional, por defecto - seis asteriscos)

Las reglas de enmascaramiento se aplican a toda la consulta (para evitar fugas de datos confidenciales de consultas mal formadas / no analizables).

`system.events` la tabla tiene contador `QueryMaskingRulesMatch` que tienen un número total de coincidencias de reglas de enmascaramiento de consultas.

Para consultas distribuidas, cada servidor debe configurarse por separado; de lo contrario, las subconsultas pasan a otros
los nodos se almacenarán sin enmascarar.

## remote\_servers {#server-settings-remote-servers}

Configuración de los clústeres utilizados por [Distribuido](../../engines/table-engines/special/distributed.md) motor de mesa y por el `cluster` función de la tabla.

**Ejemplo**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

Para el valor de la `incl` atributo, consulte la sección “[Archivos de configuración](../configuration-files.md#configuration_files)”.

**Ver también**

-   [skip\_unavailable\_shards](../settings/settings.md#settings-skip_unavailable_shards)

## Zona horaria {#server_configuration_parameters-timezone}

La zona horaria del servidor.

Especificado como un identificador de la IANA para la zona horaria UTC o la ubicación geográfica (por ejemplo, África/Abidjan).

La zona horaria es necesaria para las conversiones entre los formatos String y DateTime cuando los campos DateTime se envían al formato de texto (impreso en la pantalla o en un archivo) y cuando se obtiene DateTime de una cadena. Además, la zona horaria se usa en funciones que funcionan con la hora y la fecha si no recibieron la zona horaria en los parámetros de entrada.

**Ejemplo**

``` xml
<timezone>Europe/Moscow</timezone>
```

## Tcp\_port {#server_configuration_parameters-tcp_port}

Puerto para comunicarse con clientes a través del protocolo TCP.

**Ejemplo**

``` xml
<tcp_port>9000</tcp_port>
```

## Tcp\_port\_secure {#server_configuration_parameters-tcp_port_secure}

Puerto TCP para una comunicación segura con los clientes. Úselo con [OpenSSL](#server_configuration_parameters-openssl) configuración.

**Valores posibles**

Entero positivo.

**Valor predeterminado**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql\_port {#server_configuration_parameters-mysql_port}

Puerto para comunicarse con clientes a través del protocolo MySQL.

**Valores posibles**

Entero positivo.

Ejemplo

``` xml
<mysql_port>9004</mysql_port>
```

## tmp\_path {#server-settings-tmp_path}

Ruta de acceso a datos temporales para procesar consultas grandes.

!!! note "Nota"
    La barra diagonal es obligatoria.

**Ejemplo**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp\_policy {#server-settings-tmp-policy}

Política de [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) para almacenar archivos temporales.
Si no se establece [`tmp_path`](#server-settings-tmp_path) se utiliza, de lo contrario se ignora.

!!! note "Nota"
    - `move_factor` se ignora
- `keep_free_space_bytes` se ignora
- `max_data_part_size_bytes` se ignora
- debe tener exactamente un volumen en esa política

## Uncompressed\_cache\_size {#server-settings-uncompressed_cache_size}

Tamaño de la memoria caché (en bytes) para los datos sin comprimir utilizados por los motores de [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md).

Hay una caché compartida para el servidor. La memoria se asigna a pedido. La caché se usa si la opción [Use\_uncompressed\_cache](../settings/settings.md#setting-use_uncompressed_cache) está habilitado.

La caché sin comprimir es ventajosa para consultas muy cortas en casos individuales.

**Ejemplo**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user\_files\_path {#server_configuration_parameters-user_files_path}

El directorio con archivos de usuario. Utilizado en la función de tabla [file()](../../sql-reference/table-functions/file.md).

**Ejemplo**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users\_config {#users-config}

Ruta de acceso al archivo que contiene:

-   Configuraciones de usuario.
-   Derechos de acceso.
-   Perfiles de configuración.
-   Configuración de cuota.

**Ejemplo**

``` xml
<users_config>users.xml</users_config>
```

## Zookeeper {#server-settings_zookeeper}

Contiene la configuración que permite a ClickHouse interactuar con [ZooKeeper](http://zookeeper.apache.org/) Cluster.

ClickHouse utiliza ZooKeeper para almacenar metadatos de réplicas cuando se utilizan tablas replicadas. Si no se utilizan tablas replicadas, se puede omitir esta sección de parámetros.

Esta sección contiene los siguientes parámetros:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    Por ejemplo:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [Znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) que se utiliza como la raíz de los znodes utilizados por el servidor ClickHouse. Opcional.
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**Ejemplo de configuración**

``` xml
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
</zookeeper>
```

**Ver también**

-   [Replicación](../../engines/table-engines/mergetree-family/replication.md)
-   [Guía del programador ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use\_minimalistic\_part\_header\_in\_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Método de almacenamiento para encabezados de parte de datos en ZooKeeper.

Esta configuración sólo se aplica a `MergeTree` familia. Se puede especificar:

-   A nivel mundial en el [merge\_tree](#server_configuration_parameters-merge_tree) sección de la `config.xml` file.

    ClickHouse utiliza la configuración para todas las tablas del servidor. Puede cambiar la configuración en cualquier momento. Las tablas existentes cambian su comportamiento cuando cambia la configuración.

-   Para cada tabla.

    Al crear una tabla, especifique la correspondiente [ajuste del motor](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). El comportamiento de una tabla existente con esta configuración no cambia, incluso si la configuración global cambia.

**Valores posibles**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

Si `use_minimalistic_part_header_in_zookeeper = 1`, entonces [repetición](../../engines/table-engines/mergetree-family/replication.md) las tablas almacenan los encabezados de las partes de datos de forma compacta `znode`. Si la tabla contiene muchas columnas, este método de almacenamiento reduce significativamente el volumen de los datos almacenados en Zookeeper.

!!! attention "Atención"
    Después de aplicar `use_minimalistic_part_header_in_zookeeper = 1`, no puede degradar el servidor ClickHouse a una versión que no admite esta configuración. Tenga cuidado al actualizar ClickHouse en servidores de un clúster. No actualice todos los servidores a la vez. Es más seguro probar nuevas versiones de ClickHouse en un entorno de prueba o solo en unos pocos servidores de un clúster.

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**Valor predeterminado:** 0.

## disable\_internal\_dns\_cache {#server-settings-disable-internal-dns-cache}

Deshabilita la memoria caché DNS interna. Recomendado para operar ClickHouse en sistemas
con infraestructura que cambia frecuentemente como Kubernetes.

**Valor predeterminado:** 0.

## dns\_cache\_update\_period {#server-settings-dns-cache-update-period}

El período de actualización de las direcciones IP almacenadas en la caché DNS interna de ClickHouse (en segundos).
La actualización se realiza de forma asíncrona, en un subproceso del sistema separado.

**Valor predeterminado**: 15.

## access\_control\_path {#access_control_path}

Ruta de acceso a una carpeta donde un servidor ClickHouse almacena configuraciones de usuario y rol creadas por comandos SQL.

Valor predeterminado: `/var/lib/clickhouse/access/`.

**Ver también**

-   [Control de acceso y gestión de cuentas](../access-rights.md#access-control)

[Artículo Original](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
