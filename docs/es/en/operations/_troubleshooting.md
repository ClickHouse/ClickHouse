---
title: Resolución de problemas
---

[//]: # "Este archivo está incluido en FAQ > Resolución de problemas"

* [Instalación](#troubleshooting-installation-errors)
* [Conexión al servidor](#troubleshooting-accepts-no-connections)
* [Procesamiento de consultas](#troubleshooting-does-not-process-queries)
* [Eficiencia del procesamiento de consultas](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## Instalación
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### No se pueden obtener paquetes deb del repositorio de ClickHouse con apt-get
</div>

* Compruebe la configuración del firewall.
* Si no puede acceder al repositorio por algún motivo, descargue los paquetes como se describe en la [guía de instalación](../getting-started/install.md) e instálelos manualmente con el comando `sudo dpkg -i <packages>`. También necesitará el paquete `tzdata`.

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### No se pueden actualizar paquetes deb del repositorio de ClickHouse con apt-get
</div>

* Este issue puede producirse cuando cambia la clave GPG.

Sigue las instrucciones de la página de [configuración](../getting-started/install.md#setup-the-debian-repository) para actualizar la configuración del repositorio.

<div id="you-get-different-warnings-with-apt-get-update">
  ### Aparecen distintas advertencias al ejecutar `apt-get update`
</div>

* Los mensajes de advertencia completos son alguno de los siguientes:

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

Para solucionar el problema anterior, utilice el siguiente script:

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### No puedes descargar paquetes con yum debido a una firma incorrecta
</div>

Posible problema: hay un error en la caché; puede que se haya dañado después de la actualización de la clave GPG en 2022-09.

La solución es limpiar la caché y el directorio lib de yum:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

Luego, sigue la [guía de instalación](../getting-started/install.md#from-rpm-packages)

<div id="you-cant-run-docker-container">
  ### No puedes ejecutar el contenedor de Docker
</div>

Estás ejecutando un simple `docker run clickhouse/clickhouse-server` y falla con una traza de pila similar a la siguiente:

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

La causa es una versión antigua del daemon de Docker, inferior a `20.10.10`. Para solucionarlo, puedes actualizarlo o ejecutar `docker run [--privileged | --security-opt seccomp=unconfined]`. Esta última opción tiene implicaciones de seguridad.

<div id="troubleshooting-accepts-no-connections">
  ## Conexión con el servidor
</div>

Posibles problemas:

* El servidor no está en funcionamiento.
* Parámetros de configuración inesperados o incorrectos.

<div id="server-is-not-running">
  ### El servidor no está en ejecución
</div>

**Comprueba si el servidor está en ejecución**

Comando:

```bash
$ sudo service clickhouse-server status
```

Si el servidor no se está ejecutando, inícielo con el comando:

```bash
$ sudo service clickhouse-server start
```

**Compruebe los logs**

El log principal de `clickhouse-server` se encuentra en `/var/log/clickhouse-server/clickhouse-server.log` de forma predeterminada.

Si el servidor se inició correctamente, debería ver las siguientes cadenas:

* `<Information> Application: starting up.` — El servidor se inició.
* `<Information> Application: Ready for connections.` — El servidor está en ejecución y listo para aceptar conexiones.

Si el inicio de `clickhouse-server` falló por un error de configuración, debería ver la cadena `<Error>` con una descripción del error. Por ejemplo:

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Si no encuentra un error al final del archivo, revise todo el archivo a partir de la cadena:

```text
<Information> Application: starting up.
```

Si intenta iniciar una segunda instancia de `clickhouse-server` en el servidor, verá el siguiente mensaje en el log:

```text
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

**Consulte los registros de system.d**

Si no encuentra ninguna información útil en los registros de `clickhouse-server` o no hay ninguno, puede ver los registros de `system.d` con el siguiente comando:

```bash
$ sudo journalctl -u clickhouse-server
```

**Iniciar clickhouse-server en modo interactivo**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Este comando inicia el servidor como una aplicación interactiva con los parámetros estándar del script de inicio automático. En este modo, `clickhouse-server` imprime todos los mensajes de evento en la consola.

<div id="configuration-parameters">
  ### Parámetros de configuración
</div>

Compruebe lo siguiente:

* La configuración de Docker.

  Si ejecuta ClickHouse en Docker en una red IPv6, asegúrese de que esté establecido `network=host`.

* La configuración del endpoint.

  Compruebe la configuración de [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) y [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port).

  De forma predeterminada, el servidor ClickHouse solo acepta conexiones desde localhost.

* La configuración del protocolo HTTP.

  Compruebe la configuración del protocolo de la API HTTP.

* La configuración de la conexión segura.

  Compruebe:

  * La configuración de [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure).
  * La configuración de los [certificados SSL](../operations/server-configuration-parameters/settings.md#openssl).

    Use los parámetros adecuados al conectarse. Por ejemplo, use el parámetro `port_secure` con `clickhouse_client`.

* La configuración del usuario.

  Es posible que esté utilizando un nombre de usuario o una contraseña incorrectos.

<div id="troubleshooting-does-not-process-queries">
  ## Procesamiento de consultas
</div>

Si ClickHouse no puede procesar la consulta, envía una descripción del error al client. En `clickhouse-client`, se muestra una descripción del error en la consola. Si usas la interfaz HTTP, ClickHouse envía la descripción del error en el cuerpo de la respuesta. Por ejemplo:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Si inicia `clickhouse-client` con el parámetro `stack-trace`, ClickHouse devuelve la traza de pila del servidor junto con la descripción del error.

Es posible que vea un mensaje sobre una conexión interrumpida. En ese caso, puede repetir la consulta. Si la conexión se interrumpe cada vez que realiza la consulta, compruebe los logs del servidor para detectar errores.

<div id="troubleshooting-too-slow">
  ## Eficiencia del procesamiento de consultas
</div>

Si ve que ClickHouse funciona con demasiada lentitud, debe perfilar la carga que sus consultas generan en los recursos del servidor y en la red.

Puede usar la utilidad clickhouse-benchmark para perfilar consultas. Muestra el número de consultas procesadas por segundo, el número de filas procesadas por segundo y los percentiles de los tiempos de procesamiento de las consultas.