---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "Soluci\xF3n de problemas"
---

# Solución de problemas {#troubleshooting}

-   [Instalación](#troubleshooting-installation-errors)
-   [Conexión al servidor](#troubleshooting-accepts-no-connections)
-   [Procesamiento de consultas](#troubleshooting-does-not-process-queries)
-   [Eficiencia del procesamiento de consultas](#troubleshooting-too-slow)

## Instalación {#troubleshooting-installation-errors}

### No puede obtener paquetes Deb del repositorio ClickHouse con Apt-get {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   Compruebe la configuración del firewall.
-   Si no puede acceder al repositorio por cualquier motivo, descargue los paquetes como se describe en el [Primeros pasos](../getting-started/index.md) artículo e instálelos manualmente usando el `sudo dpkg -i <packages>` comando. También necesitará el `tzdata` paquete.

## Conexión al servidor {#troubleshooting-accepts-no-connections}

Posibles problemas:

-   El servidor no se está ejecutando.
-   Parámetros de configuración inesperados o incorrectos.

### El servidor no se está ejecutando {#server-is-not-running}

**Compruebe si el servidor está ejecutado**

Comando:

``` bash
$ sudo service clickhouse-server status
```

Si el servidor no se está ejecutando, inícielo con el comando:

``` bash
$ sudo service clickhouse-server start
```

**Comprobar registros**

El registro principal de `clickhouse-server` está en `/var/log/clickhouse-server/clickhouse-server.log` predeterminada.

Si el servidor se inició correctamente, debería ver las cadenas:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

Si `clickhouse-server` error de inicio con un error de configuración, debería ver el `<Error>` cadena con una descripción de error. Por ejemplo:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Si no ve un error al final del archivo, revise todo el archivo a partir de la cadena:

``` text
<Information> Application: starting up.
```

Si intenta iniciar una segunda instancia de `clickhouse-server` en el servidor, verá el siguiente registro:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**Ver sistema.d registros**

Si no encuentra ninguna información útil en `clickhouse-server` registros o no hay registros, puede ver `system.d` registros usando el comando:

``` bash
$ sudo journalctl -u clickhouse-server
```

**Iniciar clickhouse-server en modo interactivo**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Este comando inicia el servidor como una aplicación interactiva con parámetros estándar del script de inicio automático. En este modo `clickhouse-server` imprime todos los mensajes de eventos en la consola.

### Parámetros de configuración {#configuration-parameters}

Comprobar:

-   Configuración de Docker.

    Si ejecuta ClickHouse en Docker en una red IPv6, asegúrese de que `network=host` se establece.

-   Configuración del punto final.

    Comprobar [listen_host](server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) y [Tcp_port](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) configuración.

    El servidor ClickHouse acepta conexiones localhost solo de forma predeterminada.

-   Configuración del protocolo HTTP.

    Compruebe la configuración del protocolo para la API HTTP.

-   Configuración de conexión segura.

    Comprobar:

    -   El [Tcp_port_secure](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) configuración.
    -   Ajustes para [Sertificados SSL](server-configuration-parameters/settings.md#server_configuration_parameters-openssl).

    Utilice los parámetros adecuados mientras se conecta. Por ejemplo, utilice el `port_secure` parámetro con `clickhouse_client`.

-   Configuración del usuario.

    Es posible que esté utilizando el nombre de usuario o la contraseña incorrectos.

## Procesamiento de consultas {#troubleshooting-does-not-process-queries}

Si ClickHouse no puede procesar la consulta, envía una descripción de error al cliente. En el `clickhouse-client` obtienes una descripción del error en la consola. Si está utilizando la interfaz HTTP, ClickHouse envía la descripción del error en el cuerpo de la respuesta. Por ejemplo:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Si empiezas `clickhouse-client` con el `stack-trace` parámetro, ClickHouse devuelve el seguimiento de la pila del servidor con la descripción de un error.

Es posible que vea un mensaje sobre una conexión rota. En este caso, puede repetir la consulta. Si la conexión se rompe cada vez que realiza la consulta, compruebe si hay errores en los registros del servidor.

## Eficiencia del procesamiento de consultas {#troubleshooting-too-slow}

Si ve que ClickHouse funciona demasiado lentamente, debe perfilar la carga en los recursos del servidor y la red para sus consultas.

Puede utilizar la utilidad clickhouse-benchmark para crear perfiles de consultas. Muestra el número de consultas procesadas por segundo, el número de filas procesadas por segundo y percentiles de tiempos de procesamiento de consultas.
