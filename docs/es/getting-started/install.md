---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 11
toc_title: "Instalaci\xF3n"
---

# Instalación {#installation}

## Requisitos del sistema {#system-requirements}

ClickHouse puede ejecutarse en cualquier Linux, FreeBSD o Mac OS X con arquitectura de CPU x86_64, AArch64 o PowerPC64LE.

Los binarios oficiales preconstruidos generalmente se compilan para x86_64 y aprovechan el conjunto de instrucciones SSE 4.2, por lo que, a menos que se indique lo contrario, el uso de la CPU que lo admite se convierte en un requisito adicional del sistema. Aquí está el comando para verificar si la CPU actual tiene soporte para SSE 4.2:

``` bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

Para ejecutar ClickHouse en procesadores que no admiten SSE 4.2 o tienen arquitectura AArch64 o PowerPC64LE, debe [construir ClickHouse a partir de fuentes](#from-sources) con los ajustes de configuración adecuados.

## Opciones de instalación disponibles {#available-installation-options}

### De paquetes DEB {#install-from-deb-packages}

Se recomienda utilizar pre-compilado oficial `deb` Paquetes para Debian o Ubuntu. Ejecute estos comandos para instalar paquetes:

``` bash
{% include 'install/deb.sh' %}
```

Si desea utilizar la versión más reciente, reemplace `stable` con `testing` (esto se recomienda para sus entornos de prueba).

También puede descargar e instalar paquetes manualmente desde [aqui](https://repo.clickhouse.tech/deb/stable/main/).

#### Paquete {#packages}

-   `clickhouse-common-static` — Installs ClickHouse compiled binary files.
-   `clickhouse-server` — Creates a symbolic link for `clickhouse-server` e instala la configuración predeterminada del servidor.
-   `clickhouse-client` — Creates a symbolic link for `clickhouse-client` y otras herramientas relacionadas con el cliente. e instala los archivos de configuración del cliente.
-   `clickhouse-common-static-dbg` — Installs ClickHouse compiled binary files with debug info.

### De paquetes RPM {#from-rpm-packages}

Se recomienda utilizar pre-compilado oficial `rpm` También puede utilizar los paquetes para CentOS, RedHat y todas las demás distribuciones de Linux basadas en rpm.

Primero, necesitas agregar el repositorio oficial:

``` bash
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/stable/x86_64
```

Si desea utilizar la versión más reciente, reemplace `stable` con `testing` (esto se recomienda para sus entornos de prueba). El `prestable` etiqueta a veces está disponible también.

A continuación, ejecute estos comandos para instalar paquetes:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

También puede descargar e instalar paquetes manualmente desde [aqui](https://repo.clickhouse.tech/rpm/stable/x86_64).

### De archivos Tgz {#from-tgz-archives}

Se recomienda utilizar pre-compilado oficial `tgz` para todas las distribuciones de Linux, donde la instalación de `deb` o `rpm` paquetes no es posible.

La versión requerida se puede descargar con `curl` o `wget` desde el repositorio https://repo.clickhouse.tech/tgz/.
Después de eso, los archivos descargados deben desempaquetarse e instalarse con scripts de instalación. Ejemplo para la última versión:

``` bash
export LATEST_VERSION=`curl https://api.github.com/repos/ClickHouse/ClickHouse/tags 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -n 1`
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-common-static-dbg-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-server-$LATEST_VERSION.tgz
curl -O https://repo.clickhouse.tech/tgz/clickhouse-client-$LATEST_VERSION.tgz

tar -xzvf clickhouse-common-static-$LATEST_VERSION.tgz
sudo clickhouse-common-static-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-common-static-dbg-$LATEST_VERSION.tgz
sudo clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh

tar -xzvf clickhouse-server-$LATEST_VERSION.tgz
sudo clickhouse-server-$LATEST_VERSION/install/doinst.sh
sudo /etc/init.d/clickhouse-server start

tar -xzvf clickhouse-client-$LATEST_VERSION.tgz
sudo clickhouse-client-$LATEST_VERSION/install/doinst.sh
```

Para los entornos de producción, se recomienda utilizar las últimas `stable`-versión. Puede encontrar su número en la página de GitHub https://github.com/ClickHouse/ClickHouse/tags con postfix `-stable`.

### Desde Docker Image {#from-docker-image}

Para ejecutar ClickHouse dentro de Docker, siga la guía en [Eje de acoplador](https://hub.docker.com/r/yandex/clickhouse-server/). Esas imágenes usan oficial `deb` paquetes dentro.

### De fuentes {#from-sources}

Para compilar manualmente ClickHouse, siga las instrucciones para [Linux](../development/build.md) o [Mac OS X](../development/build-osx.md).

Puede compilar paquetes e instalarlos o usar programas sin instalar paquetes. Además, al construir manualmente, puede deshabilitar el requisito de SSE 4.2 o compilar para CPU AArch64.

      Client: programs/clickhouse-client
      Server: programs/clickhouse-server

Tendrá que crear carpetas de datos y metadatos y `chown` para el usuario deseado. Sus rutas se pueden cambiar en la configuración del servidor (src/programs/server/config.xml), por defecto son:

      /opt/clickhouse/data/default/
      /opt/clickhouse/metadata/default/

En Gentoo, puedes usar `emerge clickhouse` para instalar ClickHouse desde fuentes.

## Lanzar {#launch}

Para iniciar el servidor como demonio, ejecute:

``` bash
$ sudo service clickhouse-server start
```

Si no tienes `service` comando ejecutar como

``` bash
$ sudo /etc/init.d/clickhouse-server start
```

Vea los registros en el `/var/log/clickhouse-server/` directorio.

Si el servidor no se inicia, compruebe las configuraciones en el archivo `/etc/clickhouse-server/config.xml`.

También puede iniciar manualmente el servidor desde la consola:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

En este caso, el registro se imprimirá en la consola, lo cual es conveniente durante el desarrollo.
Si el archivo de configuración está en el directorio actual, no es necesario `--config-file` parámetro. De forma predeterminada, utiliza `./config.xml`.

ClickHouse admite la configuración de restricción de acceso. Están ubicados en el `users.xml` archivo (junto a `config.xml`).
De forma predeterminada, se permite el acceso desde cualquier lugar `default` usuario, sin una contraseña. Ver `user/default/networks`.
Para obtener más información, consulte la sección [“Configuration Files”](../operations/configuration-files.md).

Después de iniciar el servidor, puede usar el cliente de línea de comandos para conectarse a él:

``` bash
$ clickhouse-client
```

Por defecto, se conecta a `localhost:9000` en nombre del usuario `default` sin una contraseña. También se puede usar para conectarse a un servidor remoto usando `--host` argumento.

El terminal debe usar codificación UTF-8.
Para obtener más información, consulte la sección [“Command-line client”](../interfaces/cli.md).

Ejemplo:

``` bash
$ ./clickhouse-client
ClickHouse client version 0.0.18749.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.18749.

:) SELECT 1

SELECT 1

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.003 sec.

:)
```

**Felicidades, el sistema funciona!**

Para continuar experimentando, puede descargar uno de los conjuntos de datos de prueba o pasar por [tutorial](https://clickhouse.tech/tutorial.html).

[Artículo Original](https://clickhouse.tech/docs/en/getting_started/install/) <!--hide-->
