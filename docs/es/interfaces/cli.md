---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 17
toc_title: "Cliente de l\xEDnea de comandos"
---

# Cliente de línea de comandos {#command-line-client}

ClickHouse proporciona un cliente de línea de comandos nativo: `clickhouse-client`. El cliente admite opciones de línea de comandos y archivos de configuración. Para obtener más información, consulte [Configuración](#interfaces_cli_configuration).

[Instalar](../getting-started/index.md) desde el `clickhouse-client` paquete y ejecútelo con el comando `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 19.17.1.1579 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.17.1 revision 54428.

:)
```

Las diferentes versiones de cliente y servidor son compatibles entre sí, pero es posible que algunas funciones no estén disponibles en clientes anteriores. Se recomienda utilizar la misma versión del cliente que la aplicación de servidor. Cuando intenta usar un cliente de la versión anterior, entonces el servidor, `clickhouse-client` muestra el mensaje:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## Uso {#cli_usage}

El cliente se puede utilizar en modo interactivo y no interactivo (por lotes). Para utilizar el modo por lotes, especifique el ‘query’ parámetro, o enviar datos a ‘stdin’ (verifica que ‘stdin’ no es un terminal), o ambos. Similar a la interfaz HTTP, cuando se utiliza el ‘query’ parámetro y el envío de datos a ‘stdin’ la solicitud es una concatenación de la ‘query’ parámetro, un avance de línea y los datos en ‘stdin’. Esto es conveniente para grandes consultas INSERT.

Ejemplo de uso del cliente para insertar datos:

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

En el modo por lotes, el formato de datos predeterminado es TabSeparated. Puede establecer el formato en la cláusula FORMAT de la consulta.

De forma predeterminada, solo puede procesar una única consulta en modo por lotes. Para realizar múltiples consultas desde un “script,” utilizar el `--multiquery` parámetro. Esto funciona para todas las consultas excepto INSERT . Los resultados de la consulta se generan consecutivamente sin separadores adicionales. Del mismo modo, para procesar un gran número de consultas, puede ejecutar ‘clickhouse-client’ para cada consulta. Tenga en cuenta que puede tomar decenas de milisegundos para iniciar el ‘clickhouse-client’ programa.

En el modo interactivo, obtiene una línea de comandos donde puede ingresar consultas.

Si ‘multiline’ no se especifica (el valor predeterminado): Para ejecutar la consulta, pulse Intro. El punto y coma no es necesario al final de la consulta. Para introducir una consulta de varias líneas, introduzca una barra invertida `\` antes de la alimentación de línea. Después de presionar Enter, se le pedirá que ingrese la siguiente línea de la consulta.

Si se especifica multilínea: Para ejecutar una consulta, finalícela con un punto y coma y presione Intro. Si se omitió el punto y coma al final de la línea ingresada, se le pedirá que ingrese la siguiente línea de la consulta.

Solo se ejecuta una sola consulta, por lo que se ignora todo después del punto y coma.

Puede especificar `\G` en lugar o después del punto y coma. Esto indica el formato vertical. En este formato, cada valor se imprime en una línea separada, lo cual es conveniente para tablas anchas. Esta característica inusual se agregó por compatibilidad con la CLI de MySQL.

La línea de comandos se basa en ‘replxx’ (similar a ‘readline’). En otras palabras, utiliza los atajos de teclado familiares y mantiene un historial. La historia está escrita para `~/.clickhouse-client-history`.

De forma predeterminada, el formato utilizado es PrettyCompact. Puede cambiar el formato en la cláusula FORMAT de la consulta o especificando `\G` al final de la consulta, utilizando el `--format` o `--vertical` en la línea de comandos, o utilizando el archivo de configuración del cliente.

Para salir del cliente, presione Ctrl+D (o Ctrl+C) o introduzca una de las siguientes opciones en lugar de una consulta: “exit”, “quit”, “logout”, “exit;”, “quit;”, “logout;”, “q”, “Q”, “:q”

Al procesar una consulta, el cliente muestra:

1.  Progreso, que se actualiza no más de 10 veces por segundo (de forma predeterminada). Para consultas rápidas, es posible que el progreso no tenga tiempo para mostrarse.
2.  La consulta con formato después del análisis, para la depuración.
3.  El resultado en el formato especificado.
4.  El número de líneas en el resultado, el tiempo transcurrido y la velocidad promedio de procesamiento de consultas.

Puede cancelar una consulta larga presionando Ctrl + C. Sin embargo, aún tendrá que esperar un poco para que el servidor aborte la solicitud. No es posible cancelar una consulta en determinadas etapas. Si no espera y presiona Ctrl + C por segunda vez, el cliente saldrá.

El cliente de línea de comandos permite pasar datos externos (tablas temporales externas) para consultar. Para obtener más información, consulte la sección “External data for query processing”.

### Consultas con parámetros {#cli-queries-with-parameters}

Puede crear una consulta con parámetros y pasarles valores desde la aplicación cliente. Esto permite evitar formatear consultas con valores dinámicos específicos en el lado del cliente. Por ejemplo:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### Sintaxis de consulta {#cli-queries-with-parameters-syntax}

Formatee una consulta como de costumbre, luego coloque los valores que desea pasar de los parámetros de la aplicación a la consulta entre llaves en el siguiente formato:

``` sql
{<name>:<data type>}
```

-   `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
-   `data type` — [Tipo de datos](../sql-reference/data-types/index.md) del valor del parámetro de la aplicación. Por ejemplo, una estructura de datos como `(integer, ('string', integer))` puede tener el `Tuple(UInt8, Tuple(String, UInt8))` tipo de datos (también puede usar otro [entero](../sql-reference/data-types/int-uint.md) tipo).

#### Ejemplo {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
```

## Configuración {#interfaces_cli_configuration}

Puede pasar parámetros a `clickhouse-client` (todos los parámetros tienen un valor predeterminado) usando:

-   Desde la línea de comandos

    Las opciones de la línea de comandos anulan los valores y valores predeterminados de los archivos de configuración.

-   Archivos de configuración.

    Los valores de los archivos de configuración anulan los valores predeterminados.

### Opciones de línea de comandos {#command-line-options}

-   `--host, -h` -– The server name, ‘localhost’ predeterminada. Puede utilizar el nombre o la dirección IPv4 o IPv6.
-   `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
-   `--user, -u` – The username. Default value: default.
-   `--password` – The password. Default value: empty string.
-   `--query, -q` – The query to process when using non-interactive mode.
-   `--database, -d` – Select the current default database. Default value: the current database from the server settings (‘default’ predeterminada).
-   `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
-   `--multiquery, -n` – If specified, allow processing multiple queries separated by semicolons.
-   `--format, -f` – Use the specified default format to output the result.
-   `--vertical, -E` – If specified, use the Vertical format by default to output the result. This is the same as ‘–format=Vertical’. En este formato, cada valor se imprime en una línea separada, lo que es útil cuando se muestran tablas anchas.
-   `--time, -t` – If specified, print the query execution time to ‘stderr’ en modo no interactivo.
-   `--stacktrace` – If specified, also print the stack trace if an exception occurs.
-   `--config-file` – The name of the configuration file.
-   `--secure` – If specified, will connect to server over secure connection.
-   `--param_<name>` — Value for a [consulta con parámetros](#cli-queries-with-parameters).

### Archivos de configuración {#configuration_files}

`clickhouse-client` utiliza el primer archivo existente de los siguientes:

-   Definido en el `--config-file` parámetro.
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

Ejemplo de un archivo de configuración:

``` xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>False</secure>
</config>
```

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/cli/) <!--hide-->
