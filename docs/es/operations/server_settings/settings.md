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

Ajustes de compresión de datos para [Método de codificación de datos:](../table_engines/mergetree.md)-mesas de motor.

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

-   `min_part_size` – El tamaño mínimo de una parte de datos.
-   `min_part_size_ratio` – La relación entre el tamaño de la parte de datos y el tamaño de la tabla.
-   `method` – Método de compresión. Valores aceptables: `lz4` o `zstd`.

Puede configurar múltiples `<case>` apartado.

Acciones cuando se cumplen las condiciones:

-   Si un elemento de datos coincide con un conjunto de condiciones, ClickHouse utiliza el método de compresión especificado.
-   Si un elemento de datos coinciden con varios conjuntos de condiciones, ClickHouse utiliza el primer conjunto de condiciones coincidente.

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

Para obtener una lista de bases de datos, [MOSTRAR BASAS DE DATOS](../../query_language/show.md#show-databases) consulta.

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

## Diccionarios\_config {#server_settings-dictionaries_config}

La ruta de acceso al archivo de configuración para diccionarios externos.

Camino:

-   Especifique la ruta absoluta o la ruta relativa al archivo de configuración del servidor.
-   La ruta puede contener comodines \* y ?.

Ver también “[Diccionarios externos](../../query_language/dicts/external_dicts.md)”.

**Ejemplo**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## Diccionarios\_lazy\_load {#server_settings-dictionaries_lazy_load}

La carga perezosa de los diccionarios.

Si `true`, entonces cada diccionario es creado en el primer uso. Si se produce un error en la creación del diccionario, la función que estaba utilizando el diccionario produce una excepción.

Si `false`, todos los diccionarios se crean cuando se inicia el servidor, y si hay un error, el servidor se apaga.

El valor predeterminado es `true`.

**Ejemplo**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format\_schema\_path {#server_settings-format_schema_path}

La ruta de acceso al directorio con los esquemas para los datos de entrada, como los esquemas [CapnProto](../../interfaces/formats.md#capnproto) formato.

**Ejemplo**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## Grafito {#server_settings-graphite}

Envío de datos a [Grafito](https://github.com/graphite-project).

Configuración:

-   host – El servidor de grafito.
-   el puerto del servidor grafito.
-   intervalo – El intervalo para el envío, en segundos.
-   timeout – El tiempo de espera para el envío de datos, en segundos.
-   root\_path – Prefijo para las claves.
-   métricas – Envío de datos desde el [sistema.métricas](../system_tables.md#system_tables-metrics) tabla.
-   eventos – Envío de datos deltas acumulados para el período de tiempo [sistema.evento](../system_tables.md#system_tables-events) tabla.
-   events\_cumulative: envío de datos acumulativos desde el [sistema.evento](../system_tables.md#system_tables-events) tabla.
-   asynchronous\_metrics – Envío de datos desde el [sistema.asynchronous\_metrics](../system_tables.md#system_tables-asynchronous_metrics) tabla.

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

## graphite\_rollup {#server_settings-graphite-rollup}

Ajustes para reducir los datos de grafito.

Para obtener más información, consulte [GraphiteMergeTree](../table_engines/graphitemergetree.md).

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

Si `https_port` se especifica, [openSSL](#server_settings-openssl) debe ser configurado.

Si `http_port` se especifica, la configuración de OpenSSL se ignora incluso si está establecida.

**Ejemplo**

``` xml
<https>0000</https>
```

## http\_server\_default\_response {#server_settings-http_server_default_response}

La página que se muestra de forma predeterminada al acceder al servidor HTTP de ClickHouse.
El valor predeterminado es “Ok.” (con un avance de línea al final)

**Ejemplo**

Abrir `https://tabix.io/` al acceder `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include\_from {#server_settings-include_from}

La ruta al archivo con sustituciones.

Para obtener más información, consulte la sección “[Archivos de configuración](../configuration_files.md#configuration_files)”.

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

## Sistema abierto. {#interserver-http-host}

El nombre de host que pueden utilizar otros servidores para acceder a este servidor.

Si se omite, se define de la misma manera que el `hostname-f` comando.

Útil para separarse de una interfaz de red específica.

**Ejemplo**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver\_http\_credentials {#server-settings-interserver-http-credentials}

El nombre de usuario y la contraseña utilizados para [replicación](../table_engines/replication.md) con los motores Replicated\*. Estas credenciales sólo se utilizan para la comunicación entre réplicas y no están relacionadas con las credenciales de los clientes de ClickHouse. El servidor está comprobando estas credenciales para conectar réplicas y utiliza las mismas credenciales cuando se conecta a otras réplicas. Por lo tanto, estas credenciales deben establecerse igual para todas las réplicas de un clúster.
De forma predeterminada, la autenticación no se utiliza.

Esta sección contiene los siguientes parámetros:

-   `user` — nombre de usuario.
-   `password` — contraseña.

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

## listen\_host {#server_settings-listen_host}

Restricción en hosts de los que pueden provenir las solicitudes. Si desea que el servidor responda a todos ellos, especifique `::`.

Ejemplos:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## registrador {#server_settings-logger}

Configuración de registro.

Claves:

-   level – Nivel de registro. Valores aceptables: `trace`, `debug`, `information`, `warning`, `error`.
-   log – El archivo de registro. Contiene todas las entradas según `level`.
-   errorlog – Archivo de registro de errores.
-   size – Tamaño del archivo. Se aplica a `log`y`errorlog`. Una vez que el archivo alcanza `size`, ClickHouse archiva y cambia el nombre, y crea un nuevo archivo de registro en su lugar.
-   count: el número de archivos de registro archivados que almacena ClickHouse.

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

-   use\_syslog — Ajuste requerido si desea escribir en el syslog.
-   address — El host\[:port\] de syslogd. Si se omite, se utiliza el daemon local.
-   hostname — Opcional. El nombre del host desde el que se envían los registros.
-   instalación — [La palabra clave syslog instalación](https://en.wikipedia.org/wiki/Syslog#Facility) en letras mayúsculas con el “LOG\_” prefijo: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3` y así sucesivamente).
    Valor predeterminado: `LOG_USER` si `address` se especifica, `LOG_DAEMON otherwise.`
-   format – Formato de mensaje. Valores posibles: `bsd` y `syslog.`

## macro {#macros}

Sustituciones de parámetros para tablas replicadas.

Se puede omitir si no se utilizan tablas replicadas.

Para obtener más información, consulte la sección “[Creación de tablas replicadas](../../operations/table_engines/replication.md)”.

**Ejemplo**

``` xml
<macros incl="macros" optional="true" />
```

## Método de codificación de datos: {#server-mark-cache-size}

Tamaño aproximado (en bytes) de la memoria caché de marcas utilizadas por los motores de [Método de codificación de datos:](../table_engines/mergetree.md) Familia.

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

Si el tamaño de un [Método de codificación de datos:](../table_engines/mergetree.md) más caliente `max_table_size_to_drop` (en bytes), no puede eliminarlo mediante una consulta DROP.

Si aún necesita eliminar la tabla sin reiniciar el servidor ClickHouse, cree el `<clickhouse-path>/flags/force_drop_table` y ejecute la consulta DROP.

Valor predeterminado: 50 GB.

El valor 0 significa que puede eliminar todas las tablas sin restricciones.

**Ejemplo**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge\_tree {#server_settings-merge_tree}

Ajuste fino para tablas en el [Método de codificación de datos:](../table_engines/mergetree.md).

Para obtener más información, vea MergeTreeSettings.h archivo de encabezado.

**Ejemplo**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_settings-openssl}

Configuración cliente/servidor SSL.

El soporte para SSL es proporcionado por el `libpoco` biblioteca. La interfaz se describe en el archivo [Nombre de la red inalámbrica (SSID):h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Claves para la configuración del servidor/cliente:

-   privateKeyFile: la ruta de acceso al archivo con la clave secreta del certificado PEM. El archivo puede contener una clave y un certificado al mismo tiempo.
-   certificateFile: la ruta de acceso al archivo de certificado cliente/servidor en formato PEM. Puede omitirlo si `privateKeyFile` contiene el certificado.
-   caConfig: la ruta de acceso al archivo o directorio que contiene certificados raíz de confianza.
-   verificationMode: el método para verificar los certificados del nodo. Los detalles están en la descripción del [Contexto](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) Clase. Valores posibles: `none`, `relaxed`, `strict`, `once`.
-   Profundidad de verificación: la longitud máxima de la cadena de verificación. La verificación fallará si la longitud de la cadena del certificado supera el valor establecido.
-   loadDefaultCAFile: indica que se usarán certificados de CA integrados para OpenSSL. Valores aceptables: `true`, `false`. \|
-   cipherList: encriptaciones compatibles con OpenSSL. Por ejemplo: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions: habilita o deshabilita las sesiones de almacenamiento en caché. Debe usarse en combinación con `sessionIdContext`. Valores aceptables: `true`, `false`.
-   sessionIdContext: un conjunto único de caracteres aleatorios que el servidor agrega a cada identificador generado. La longitud de la cuerda no debe exceder `SSL_MAX_SSL_SESSION_ID_LENGTH`. Este parámetro siempre se recomienda ya que ayuda a evitar problemas tanto si el servidor almacena en caché la sesión como si el cliente solicita el almacenamiento en caché. Valor predeterminado: `${application.name}`.
-   sessionCacheSize: el número máximo de sesiones que el servidor almacena en caché. Valor predeterminado: 1024\*20. 0 – Sesiones ilimitadas.
-   sessionTimeout - Tiempo para almacenar en caché la sesión en el servidor.
-   extendedVerification : la verificación extendida automáticamente de los certificados después de que finalice la sesión. Valores aceptables: `true`, `false`.
-   requireTLSv1: requiere una conexión TLSv1. Valores aceptables: `true`, `false`.
-   requireTLSv1\_1: requiere una conexión TLSv1.1. Valores aceptables: `true`, `false`.
-   requireTLSv1: requiere una conexión TLSv1.2. Valores aceptables: `true`, `false`.
-   fips: activa el modo FIPS OpenSSL. Se admite si la versión OpenSSL de la biblioteca admite FIPS.
-   privateKeyPassphraseHandler: clase (subclase PrivateKeyPassphraseHandler) que solicita la frase de contraseña para acceder a la clave privada. Por ejemplo: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler: clase (una subclase de CertificateHandler) para verificar certificados no válidos. Por ejemplo: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols: protocolos que no pueden usarse.
-   preferServerCiphers: cifras de servidor preferidas en el cliente.

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

## part\_log {#server_settings-part-log}

Registro de eventos asociados con [Método de codificación de datos:](../table_engines/mergetree.md). Por ejemplo, agregar o fusionar datos. Puede utilizar el registro para simular algoritmos de combinación y comparar sus características. Puede visualizar el proceso de fusión.

Las consultas se registran en el [sistema.part\_log](../system_tables.md#system_tables-part-log) tabla, no en un archivo separado. Puede configurar el nombre de esta tabla en el `table` parámetro (ver más abajo).

Utilice los siguientes parámetros para configurar el registro:

-   `database` – Nombre de la base de datos.
-   `table` – Nombre de la tabla del sistema.
-   `partition_by` – Establece un [clave de partición personalizada](../../operations/table_engines/custom_partitioning_key.md).
-   `flush_interval_milliseconds` – Intervalo para el vaciado de datos desde el búfer en la memoria a la tabla.

**Ejemplo**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## camino {#server_settings-path}

La ruta de acceso al directorio que contiene los datos.

!!! note "Nota"
    La barra diagonal es obligatoria.

**Ejemplo**

``` xml
<path>/var/lib/clickhouse/</path>
```

## query\_log {#server_settings-query-log}

Configuración de las consultas de registro recibidas con [log\_queries=1](../settings/settings.md) configuración.

Las consultas se registran en el [sistema.query\_log](../system_tables.md#system_tables-query_log) tabla, no en un archivo separado. Puede cambiar el nombre de la tabla en el `table` parámetro (ver más abajo).

Utilice los siguientes parámetros para configurar el registro:

-   `database` – Nombre de la base de datos.
-   `table` – Nombre de la tabla del sistema en la que se registrarán las consultas.
-   `partition_by` – Establece un [clave de partición personalizada](../../operations/table_engines/custom_partitioning_key.md) para una mesa.
-   `flush_interval_milliseconds` – Intervalo para el vaciado de datos desde el búfer en la memoria a la tabla.

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

## Sistema abierto. {#server_settings-query-thread-log}

Configuración de subprocesos de registro de consultas recibidas con [Log\_query\_threads = 1](../settings/settings.md#settings-log-query-threads) configuración.

Las consultas se registran en el [sistema.Sistema abierto.](../system_tables.md#system_tables-query-thread-log) tabla, no en un archivo separado. Puede cambiar el nombre de la tabla en el `table` parámetro (ver más abajo).

Utilice los siguientes parámetros para configurar el registro:

-   `database` – Nombre de la base de datos.
-   `table` – Nombre de la tabla del sistema en la que se registrarán las consultas.
-   `partition_by` – Establece un [clave de partición personalizada](../../operations/table_engines/custom_partitioning_key.md) para una tabla del sistema.
-   `flush_interval_milliseconds` – Intervalo para el vaciado de datos desde el búfer en la memoria a la tabla.

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

## trace\_log {#server_settings-trace_log}

Ajustes para el [trace\_log](../system_tables.md#system_tables-trace_log) operación de la tabla del sistema.

Parámetros:

-   `database` — Base de datos para almacenar una tabla.
-   `table` — Nombre de la tabla.
-   `partition_by` — [Clave de partición personalizada](../../operations/table_engines/custom_partitioning_key.md) para una tabla del sistema.
-   `flush_interval_milliseconds` — Intervalo para el vaciado de datos del búfer en la memoria a la tabla.

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

Configuración de los clústeres utilizados por [Distribuido](../../operations/table_engines/distributed.md) motor de mesa y por el `cluster` función de la tabla.

**Ejemplo**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

Para el valor de la `incl` atributo, consulte la sección “[Archivos de configuración](../configuration_files.md#configuration_files)”.

**Ver también**

-   [skip\_unavailable\_shards](../settings/settings.md#settings-skip_unavailable_shards)

## Zona horaria {#server_settings-timezone}

La zona horaria del servidor.

Especificado como un identificador de la IANA para la zona horaria UTC o la ubicación geográfica (por ejemplo, África/Abidjan).

La zona horaria es necesaria para las conversiones entre los formatos String y DateTime cuando los campos DateTime se envían al formato de texto (impreso en la pantalla o en un archivo) y cuando se obtiene DateTime de una cadena. Además, la zona horaria se usa en funciones que funcionan con la hora y la fecha si no recibieron la zona horaria en los parámetros de entrada.

**Ejemplo**

``` xml
<timezone>Europe/Moscow</timezone>
```

## Tcp\_port {#server_settings-tcp_port}

Puerto para comunicarse con clientes a través del protocolo TCP.

**Ejemplo**

``` xml
<tcp_port>9000</tcp_port>
```

## Tcp\_port\_secure {#server_settings-tcp_port-secure}

Puerto TCP para una comunicación segura con los clientes. Úselo con [OpenSSL](#server_settings-openssl) configuración.

**Valores posibles**

Entero positivo.

**Valor predeterminado**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql\_port {#server_settings-mysql_port}

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

Política de [`storage_configuration`](../table_engines/mergetree.md#table_engine-mergetree-multiple-volumes) para almacenar archivos temporales.
Si no se establece [`tmp_path`](#server-settings-tmp_path) se utiliza, de lo contrario se ignora.

!!! note "Nota"
    - `move_factor` se ignora
- `keep_free_space_bytes` se ignora
- `max_data_part_size_bytes` se ignora
- debe tener exactamente un volumen en esa política

## Uncompressed\_cache\_size {#server-settings-uncompressed_cache_size}

Tamaño de la memoria caché (en bytes) para los datos sin comprimir utilizados por los motores de [Método de codificación de datos:](../table_engines/mergetree.md).

Hay una caché compartida para el servidor. La memoria se asigna a pedido. La caché se usa si la opción [Use\_uncompressed\_cache](../settings/settings.md#setting-use_uncompressed_cache) está habilitado.

La caché sin comprimir es ventajosa para consultas muy cortas en casos individuales.

**Ejemplo**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user\_files\_path {#server_settings-user_files_path}

El directorio con archivos de usuario. Utilizado en la función de tabla [file()](../../query_language/table_functions/file.md).

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

-   `node` — Punto final ZooKeeper. Puede establecer varios puntos finales.

    Por ejemplo:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Tiempo de espera máximo para la sesión del cliente en milisegundos.
-   `root` — El [Znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) que se utiliza como la raíz de los znodes utilizados por el servidor ClickHouse. Opcional.
-   `identity` — Usuario y contraseña, que puede ser requerido por ZooKeeper para dar acceso a los znodes solicitados. Opcional.

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

-   [Replicación](../../operations/table_engines/replication.md)
-   [Guía del programador ZooKeeper](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use\_minimalistic\_part\_header\_in\_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Método de almacenamiento para encabezados de parte de datos en ZooKeeper.

Esta configuración sólo se aplica a `MergeTree` Familia. Se puede especificar:

-   A nivel mundial en el [merge\_tree](#server_settings-merge_tree) sección de la `config.xml` file.

    ClickHouse utiliza la configuración para todas las tablas del servidor. Puede cambiar la configuración en cualquier momento. Las tablas existentes cambian su comportamiento cuando cambia la configuración.

-   Para cada tabla.

    Al crear una tabla, especifique la correspondiente [ajuste del motor](../table_engines/mergetree.md#table_engine-mergetree-creating-a-table). El comportamiento de una tabla existente con esta configuración no cambia, incluso si la configuración global cambia.

**Valores posibles**

-   0 — Funcionalidad está desactivada.
-   1 — Funcionalidad está activada.

Si `use_minimalistic_part_header_in_zookeeper = 1`, entonces [repetición](../table_engines/replication.md) las tablas almacenan los encabezados de las partes de datos de forma compacta `znode`. Si la tabla contiene muchas columnas, este método de almacenamiento reduce significativamente el volumen de los datos almacenados en Zookeeper.

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

[Artículo Original](https://clickhouse.tech/docs/es/operations/server_settings/settings/) <!--hide-->
