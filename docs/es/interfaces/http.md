---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 19
toc_title: Interfaz HTTP
---

# Interfaz HTTP {#http-interface}

La interfaz HTTP le permite usar ClickHouse en cualquier plataforma desde cualquier lenguaje de programación. Lo usamos para trabajar desde Java y Perl, así como scripts de shell. En otros departamentos, la interfaz HTTP se usa desde Perl, Python y Go. La interfaz HTTP es más limitada que la interfaz nativa, pero tiene una mejor compatibilidad.

De forma predeterminada, clickhouse-server escucha HTTP en el puerto 8123 (esto se puede cambiar en la configuración).

Si realiza una solicitud GET / sin parámetros, devuelve 200 códigos de respuesta y la cadena que definió en [http\_server\_default\_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response) valor predeterminado “Ok.” (con un avance de línea al final)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

Use la solicitud GET / ping en los scripts de comprobación de estado. Este controlador siempre devuelve “Ok.” (con un avance de línea al final). Disponible a partir de la versión 18.12.13.

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

Enviar la solicitud como una URL ‘query’ parámetro, o como un POST. O envíe el comienzo de la consulta en el ‘query’ parámetro, y el resto en el POST (explicaremos más adelante por qué esto es necesario). El tamaño de la URL está limitado a 16 KB, así que tenga esto en cuenta al enviar consultas grandes.

Si tiene éxito, recibirá el código de respuesta 200 y el resultado en el cuerpo de respuesta.
Si se produce un error, recibirá el código de respuesta 500 y un texto de descripción de error en el cuerpo de la respuesta.

Al usar el método GET, ‘readonly’ se establece. En otras palabras, para consultas que modifican datos, solo puede usar el método POST. Puede enviar la consulta en sí misma en el cuerpo POST o en el parámetro URL.

Ejemplos:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -O- -q 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Date: Wed, 27 Nov 2019 10:30:18 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
X-ClickHouse-Server-Display-Name: clickhouse.ru-central1.internal
X-ClickHouse-Query-Id: 5abe861c-239c-467f-b955-8a201abb8b7f
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}

1
```

Como puede ver, curl es algo inconveniente ya que los espacios deben ser URL escapadas.
Aunque wget escapa de todo en sí, no recomendamos usarlo porque no funciona bien sobre HTTP 1.1 cuando se usa keep-alive y Transfer-Encoding: chunked .

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

Si se envía parte de la consulta en el parámetro y parte en el POST, se inserta un avance de línea entre estas dos partes de datos.
Ejemplo (esto no funcionará):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

De forma predeterminada, los datos se devuelven en formato TabSeparated (para obtener más información, “Formats” apartado).
Utilice la cláusula FORMAT de la consulta para solicitar cualquier otro formato.

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

El método POST de transmitir datos es necesario para las consultas INSERT. En este caso, puede escribir el comienzo de la consulta en el parámetro URL y usar POST para pasar los datos a insertar. Los datos a insertar podrían ser, por ejemplo, un volcado separado por tabuladores de MySQL. De esta manera, la consulta INSERT reemplaza LOAD DATA LOCAL INFILE de MySQL.

Ejemplos: Crear una tabla:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

Uso de la consulta INSERT familiar para la inserción de datos:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

Los datos se pueden enviar por separado de la consulta:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

Puede especificar cualquier formato de datos. El ‘Values’ el formato es el mismo que el que se usa al escribir INSERT INTO t VALUES:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

Para insertar datos de un volcado separado por tabuladores, especifique el formato correspondiente:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

Lectura del contenido de la tabla. Los datos se emiten en orden aleatorio debido al procesamiento de consultas paralelas:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
7
8
9
10
11
12
1
2
3
4
5
6
```

Eliminando la mesa.

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

Para las solicitudes correctas que no devuelven una tabla de datos, se devuelve un cuerpo de respuesta vacío.

Puede utilizar el formato interno de compresión ClickHouse al transmitir datos. Los datos comprimidos tienen un formato no estándar, y deberá usar el `clickhouse-compressor` programa para trabajar con él (se instala con el `clickhouse-client` paquete). Para aumentar la eficiencia de la inserción de datos, puede deshabilitar la verificación de suma de comprobación [http\_native\_compression\_disable\_checksumming\_on\_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress) configuración.

Si ha especificado `compress=1` en la URL, el servidor comprime los datos que le envía.
Si ha especificado `decompress=1` en la dirección URL, el servidor descomprime los mismos datos que `POST` método.

También puede optar por utilizar [Compresión HTTP](https://en.wikipedia.org/wiki/HTTP_compression). Para enviar un `POST` solicitud, agregue el encabezado de solicitud `Content-Encoding: compression_method`. Para que ClickHouse comprima la respuesta, debe agregar `Accept-Encoding: compression_method`. Soporta ClickHouse `gzip`, `br`, y `deflate` [métodos de compresión](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens). Para habilitar la compresión HTTP, debe usar ClickHouse [enable\_http\_compression](../operations/settings/settings.md#settings-enable_http_compression) configuración. Puede configurar el nivel de compresión de datos [http\_zlib\_compression\_level](#settings-http_zlib_compression_level) para todos los métodos de compresión.

Puede usar esto para reducir el tráfico de red al transmitir una gran cantidad de datos o para crear volcados que se comprimen inmediatamente.

Ejemplos de envío de datos con compresión:

``` bash
#Sending data to the server:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

#Sending data to the client:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "Nota"
    Algunos clientes HTTP pueden descomprimir datos del servidor de forma predeterminada (con `gzip` y `deflate`) y puede obtener datos descomprimidos incluso si usa la configuración de compresión correctamente.

Puede usar el ‘database’ Parámetro URL para especificar la base de datos predeterminada.

``` bash
$ echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

De forma predeterminada, la base de datos que está registrada en la configuración del servidor se utiliza como base de datos predeterminada. De forma predeterminada, esta es la base de datos llamada ‘default’. Como alternativa, siempre puede especificar la base de datos utilizando un punto antes del nombre de la tabla.

El nombre de usuario y la contraseña se pueden indicar de una de estas tres maneras:

1.  Uso de la autenticación básica HTTP. Ejemplo:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  En el ‘user’ y ‘password’ Parámetros de URL. Ejemplo:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  Utilizar ‘X-ClickHouse-User’ y ‘X-ClickHouse-Key’ cabecera. Ejemplo:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

Si no se especifica el nombre de usuario, `default` se utiliza el nombre. Si no se especifica la contraseña, se utiliza la contraseña vacía.
También puede utilizar los parámetros de URL para especificar cualquier configuración para procesar una sola consulta o perfiles completos de configuración. Ejemplo:http://localhost:8123/?perfil=web&max\_rows\_to\_read=1000000000&consulta=SELECCIONA+1

Para obtener más información, consulte [Configuración](../operations/settings/index.md) apartado.

``` bash
$ echo 'SELECT number FROM system.numbers LIMIT 10' | curl 'http://localhost:8123/?' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

Para obtener información sobre otros parámetros, consulte la sección “SET”.

Del mismo modo, puede utilizar sesiones ClickHouse en el protocolo HTTP. Para hacer esto, debe agregar el `session_id` GET parámetro a la solicitud. Puede usar cualquier cadena como ID de sesión. De forma predeterminada, la sesión finaliza después de 60 segundos de inactividad. Para cambiar este tiempo de espera, modifique `default_session_timeout` configuración en la configuración del servidor, o `session_timeout` GET parámetro a la solicitud. Para comprobar el estado de la sesión, `session_check=1` parámetro. Solo se puede ejecutar una consulta a la vez en una sola sesión.

Puede recibir información sobre el progreso de una consulta en `X-ClickHouse-Progress` encabezados de respuesta. Para hacer esto, habilite [send\_progress\_in\_http\_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). Ejemplo de la secuencia de encabezado:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

Posibles campos de encabezado:

-   `read_rows` — Number of rows read.
-   `read_bytes` — Volume of data read in bytes.
-   `total_rows_to_read` — Total number of rows to be read.
-   `written_rows` — Number of rows written.
-   `written_bytes` — Volume of data written in bytes.

Las solicitudes en ejecución no se detienen automáticamente si se pierde la conexión HTTP. El análisis y el formato de datos se realizan en el lado del servidor, y el uso de la red puede ser ineficaz.
Opcional ‘query\_id’ parámetro se puede pasar como el ID de consulta (cualquier cadena). Para obtener más información, consulte la sección “Settings, replace\_running\_query”.

Opcional ‘quota\_key’ parámetro se puede pasar como la clave de cuota (cualquier cadena). Para obtener más información, consulte la sección “Quotas”.

La interfaz HTTP permite pasar datos externos (tablas temporales externas) para consultar. Para obtener más información, consulte la sección “External data for query processing”.

## Almacenamiento En búfer De Respuesta {#response-buffering}

Puede habilitar el almacenamiento en búfer de respuestas en el lado del servidor. El `buffer_size` y `wait_end_of_query` Los parámetros URL se proporcionan para este propósito.

`buffer_size` determina el número de bytes en el resultado para almacenar en búfer en la memoria del servidor. Si un cuerpo de resultado es mayor que este umbral, el búfer se escribe en el canal HTTP y los datos restantes se envían directamente al canal HTTP.

Para asegurarse de que toda la respuesta se almacena en búfer, establezca `wait_end_of_query=1`. En este caso, los datos que no se almacenan en la memoria se almacenarán en un archivo de servidor temporal.

Ejemplo:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

Utilice el almacenamiento en búfer para evitar situaciones en las que se produjo un error de procesamiento de consultas después de enviar al cliente el código de respuesta y los encabezados HTTP. En esta situación, se escribe un mensaje de error al final del cuerpo de la respuesta y, en el lado del cliente, el error solo se puede detectar en la etapa de análisis.

### Consultas Con parámetros {#cli-queries-with-parameters}

Puede crear una consulta con parámetros y pasar valores para ellos desde los parámetros de solicitud HTTP correspondientes. Para obtener más información, consulte [Consultas con parámetros para CLI](cli.md#cli-queries-with-parameters).

### Ejemplo {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## Interfaz HTTP Predefinida {#predefined_http_interface}

ClickHouse admite consultas específicas a través de la interfaz HTTP. Por ejemplo, puede escribir datos en una tabla de la siguiente manera:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse también es compatible con la interfaz HTTP predefinida que puede ayudarle a una integración más fácil con herramientas de terceros como [Prometheus exportador](https://github.com/percona-lab/clickhouse_exporter).

Ejemplo:

-   En primer lugar, agregue esta sección al archivo de configuración del servidor:

<!-- -->

``` xml
<http_handlers>
  <predefine_query_handler>
      <url>/metrics</url>
        <method>GET</method>
        <queries>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </queries>
  </predefine_query_handler>
</http_handlers>
```

-   Ahora puede solicitar la url directamente para los datos en el formato Prometheus:

<!-- -->

``` bash
curl -vvv 'http://localhost:8123/metrics'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /metrics HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Wed, 27 Nov 2019 08:54:25 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-tl62qd0o
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: f39235f6-6ed7-488c-ae07-c7ceafb960f6
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
# HELP "Query" "Number of executing queries"
# TYPE "Query" counter
"Query" 1

# HELP "Merge" "Number of executing background merges"
# TYPE "Merge" counter
"Merge" 0

# HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
# TYPE "PartMutation" counter
"PartMutation" 0

# HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
# TYPE "ReplicatedFetch" counter
"ReplicatedFetch" 0

# HELP "ReplicatedSend" "Number of data parts being sent to replicas"
# TYPE "ReplicatedSend" counter
"ReplicatedSend" 0

* Connection #0 to host localhost left intact
```

Como puede ver en el ejemplo, si `<http_handlers>` está configurado en la configuración.archivo xml, ClickHouse coincidirá con las solicitudes HTTP recibidas con el tipo predefinido en `<http_handlers>`, entonces ClickHouse ejecutará la consulta predefinida correspondiente si la coincidencia es exitosa.

Ahora `<http_handlers>` puede configurar `<root_handler>`, `<ping_handler>`, `<replicas_status_handler>`, `<dynamic_query_handler>` y `<no_handler_description>` .

## root\_handler {#root_handler}

`<root_handler>` devuelve el contenido especificado para la solicitud de ruta de acceso raíz. El contenido devuelto específico se configura mediante `http_server_default_response` en la configuración.XML. si no se especifica, devolver **Ok.**

`http_server_default_response` no está definido y se envía una solicitud HTTP a ClickHouse. El resultado es el siguiente:

``` xml
<http_handlers>
    <root_handler/>
</http_handlers>
```

    $ curl 'http://localhost:8123'
    Ok.

`http_server_default_response` se define y se envía una solicitud HTTP a ClickHouse. El resultado es el siguiente:

``` xml
<http_server_default_response><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></http_server_default_response>

<http_handlers>
    <root_handler/>
</http_handlers>
```

    $ curl 'http://localhost:8123'
    <html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%

## Método De codificación De Datos: {#ping_handler}

`<ping_handler>` se puede utilizar para sondear el estado del servidor ClickHouse actual. Cuando el servidor HTTP ClickHouse es normal, acceder a ClickHouse a través de `<ping_handler>` volverá **Ok.**.

Ejemplo:

``` xml
<http_handlers>
    <ping_handler>/ping</ping_handler>
</http_handlers>
```

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

## Sistema Abierto {#replicas_status_handler}

`<replicas_status_handler>` se utiliza para detectar el estado de la réplica y el nodo de retorno **Ok.** si el nodo de réplica no tiene retraso. Si hay un retraso, devuelva el retraso específico. El valor de `<replicas_status_handler>` admite personalización. Si no especifica `<replicas_status_handler>`, Configuración predeterminada de ClickHouse `<replicas_status_handler>` ser **/replicas\_status**.

Ejemplo:

``` xml
<http_handlers>
    <replicas_status_handler>/replicas_status</replicas_status_handler>
</http_handlers>
```

Ningún caso del retraso:

``` bash
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

Caso retrasado:

``` bash
$ curl 'http://localhost:8123/replicas_status'
db.stats:  Absolute delay: 22. Relative delay: 22.
```

## DirecciÃ³n De Correo electrÃ³nico {#predefined_query_handler}

Puede configurar `<method>`, `<headers>`, `<url>` y `<queries>` en `<predefined_query_handler>`.

`<method>` es responsable de hacer coincidir la parte del método de la solicitud HTTP. `<method>` se ajusta plenamente a la definición de [método](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) en el protocolo HTTP. Es una configuración opcional. Si no está definido en el archivo de configuración, no coincide con la parte del método de la solicitud HTTP

`<url>` es responsable de hacer coincidir la parte url de la solicitud HTTP. Es compatible con [RE2](https://github.com/google/re2)expresiones regulares. Es una configuración opcional. Si no está definido en el archivo de configuración, no coincide con la parte url de la solicitud HTTP

`<headers>` es responsable de hacer coincidir la parte del encabezado de la solicitud HTTP. Es compatible con las expresiones regulares de RE2. Es una configuración opcional. Si no está definido en el archivo de configuración, no coincide con la parte de encabezado de la solicitud HTTP

`<queries>` valor es una consulta predefinida de `<predefined_query_handler>`, que es ejecutado por ClickHouse cuando se hace coincidir una solicitud HTTP y se devuelve el resultado de la consulta. Es una configuración imprescindible.

`<predefined_query_handler>` admite la configuración de valores Settings y query\_params.

En el ejemplo siguiente se definen los valores de `max_threads` y `max_alter_threads` configuración, a continuación, consulta la tabla del sistema para comprobar si estos ajustes se han establecido correctamente.

Ejemplo:

``` xml
<root_handlers>
    <predefined_query_handler>
        <method>GET</method>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <queries>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </queries>
    </predefined_query_handler>
</root_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads   2
```

!!! note "Nota"
    En uno `<predefined_query_handler>`, una `<queries>` sólo es compatible con uno `<query>` de un tipo de plaquita.

## Nombre De La Red inalámbrica (SSID): {#dynamic_query_handler}

`<dynamic_query_handler>` que `<predefined_query_handler>` aumentar `<query_param_name>` .

ClickHouse extrae y ejecuta el valor correspondiente al `<query_param_name>` valor en la url de la petición HTTP.
Configuración predeterminada de ClickHouse `<query_param_name>` ser `/query` . Es una configuración opcional. Si no hay una definición en el archivo de configuración, el parámetro no se pasa.

Para experimentar con esta funcionalidad, el ejemplo define los valores de max\_threads y max\_alter\_threads y consulta si la configuración se estableció correctamente.
La diferencia es que en `<predefined_query_handler>`, consulta se escribe en el archivo de configuración. Pero en `<dynamic_query_handler>`, consulta se escribe en forma de param de la solicitud HTTP.

Ejemplo:

``` xml
<root_handlers>
    <dynamic_query_handler>
        <headers>
            <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>
            <PARAMS_XXX><![CDATA[(?P<param_name_1>[^/]+)(/(?P<param_name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <query_param_name>query_param</query_param_name>
    </dynamic_query_handler>
</root_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/?query_param=SELECT%20value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D&max_threads=1&max_alter_threads=2&param_name_2=max_alter_threads'
1
2
```

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/http_interface/) <!--hide-->
