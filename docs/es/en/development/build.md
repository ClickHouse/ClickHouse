---
description: 'Guía paso a paso para compilar ClickHouse desde el código fuente en sistemas Linux'
sidebar_label: 'Compilar en Linux'
sidebar_position: 10
slug: /development/build
title: 'Cómo compilar ClickHouse en Linux'
doc_type: 'guide'
---

:::info Esta guía de compilación está dirigida a colaboradores que modifican el propio ClickHouse.
Si no vas a cambiar el código fuente de ClickHouse, puedes instalar una versión precompilada de ClickHouse como se describe en [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

ClickHouse se puede compilar en las siguientes plataformas:

* x86&#95;64
* AArch64
* PowerPC 64 LE (experimental)
* s390/x (experimental)
* RISC-V 64 (experimental)

<div id="assumptions">
  ## Supuestos
</div>

El siguiente tutorial se basa en Ubuntu Linux, pero también debería funcionar en cualquier otra distribución de Linux con los ajustes adecuados.
La versión mínima recomendada de Ubuntu para desarrollo es 24.04 LTS.

Este tutorial da por hecho que ya tienes descargados localmente el repositorio de ClickHouse y todos los submódulos.

<div id="install-prerequisites">
  ## Instalar los requisitos previos
</div>

Primero, consulte la [documentación general sobre requisitos previos](developer-instruction.md).

ClickHouse usa CMake y Ninja para compilar.

Opcionalmente, puede instalar ccache para que la compilación reutilice archivos objeto ya compilados.

```bash
sudo apt-get update
sudo apt-get install build-essential git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg
```

<div id="install-the-clang-compiler">
  ## Instale el compilador Clang
</div>

Para instalar Clang en Ubuntu/Debian, utilice el script de instalación automática de LLVM disponible [aquí](https://apt.llvm.org/).

```bash
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 21
```

Para otras distribuciones de Linux, verifica si puedes instalar alguno de los [paquetes precompilados](https://releases.llvm.org/download.html) de LLVM.

A febrero de 2026, se requiere Clang 21 o superior.
GCC u otros compiladores no son compatibles.

<div id="install-the-rust-compiler-optional">
  ## Instalar el compilador de Rust (opcional)
</div>

:::note
Rust es una dependencia opcional de ClickHouse.
Si Rust no está instalado, algunas funcionalidades de ClickHouse se omitirán en la compilación.
:::

Primero, sigue los pasos de la [documentación oficial de Rust](https://www.rust-lang.org/tools/install) para instalar `rustup`.

Al igual que con las dependencias de C++, ClickHouse usa vendoring para controlar exactamente qué se instala y evitar depender de servicios de terceros (como el repositorio `crates.io`).

Aunque en modo release cualquier versión moderna del toolchain de rustup debería funcionar con estas dependencias, si planeas habilitar sanitizadores, debes usar una versión que coincida exactamente con el mismo `std` que se usa en CI (para el que hacemos vendoring de los crates):

```bash
rustup toolchain install nightly-2026-03-22
rustup default nightly-2026-03-22
rustup component add rust-src
```

<div id="build-clickhouse">
  ## Compilar ClickHouse
</div>

Recomendamos crear un directorio aparte, `build`, dentro de `ClickHouse` que contenga todos los artefactos de compilación:

```sh
mkdir build
cd build
```

Puede tener varios directorios diferentes (p. ej., `build_release`, `build_debug`, etc.) para distintos tipos de compilación.

Opcional: si tiene instaladas varias versiones del compilador, puede indicar el compilador exacto que desea usar.

```sh
export CC=clang-21
export CXX=clang++-21
```

Para tareas de desarrollo, se recomiendan las compilaciones de depuración.
En comparación con las compilaciones de release, tienen un nivel de optimización del compilador (`-O`) menor, lo que ofrece una mejor experiencia de depuración.
Además, las excepciones internas de tipo `LOGICAL_ERROR` hacen que el proceso se cierre inmediatamente en lugar de gestionarse de forma controlada.

```sh
cmake -D CMAKE_BUILD_TYPE=Debug ..
```

:::note
Si deseas usar un depurador como gdb, añade `-D DEBUG_O_LEVEL="0"` al comando anterior para eliminar todas las optimizaciones del compilador, ya que pueden interferir con la capacidad de gdb de ver o acceder a las variables.
:::

Ejecuta ninja para compilar:

```sh
ninja clickhouse
```

Si desea compilar todos los binarios (utilidades y pruebas), ejecute ninja sin parámetros:

```sh
ninja
```

Puede controlar el número de trabajos de compilación en paralelo con el parámetro `-j`:

```sh
ninja -j 1 clickhouse
```

:::note
`clickhouse-server`, `clickhouse-client` y otros binarios similares son enlaces simbólicos en el directorio `programs/` que apuntan al ejecutable `clickhouse` una vez completada la compilación.

:::tip
CMake proporciona accesos directos para los comandos anteriores:

```sh
cmake -S . -B build  # configure build, run from repository top-level directory
cmake --build build  # compile
```

:::

<div id="running-the-clickhouse-executable">
  ## Ejecutar el ejecutable de ClickHouse
</div>

Una vez que la compilación se haya completado correctamente, encontrarás el ejecutable en `ClickHouse/<build_dir>/programs/`:

El servidor de ClickHouse intenta encontrar un archivo de configuración `config.xml` en el directorio actual.
Como alternativa, puedes especificar un archivo de configuración en la línea de comandos con `-C`.

Para conectarte al servidor de ClickHouse con `clickhouse-client`, abre otra terminal, ve a `ClickHouse/build/programs/` y ejecuta `./clickhouse client`.

Si aparece el mensaje `Connection refused` en macOS o FreeBSD, prueba a especificar `127.0.0.1` como dirección del host:

```bash
clickhouse client --host 127.0.0.1
```

<div id="advanced-options">
  ## Opciones avanzadas
</div>

<div id="minimal-build">
  ### Compilación mínima
</div>

Si no necesitas la funcionalidad que proporcionan las bibliotecas de terceros, puedes acelerar aún más la compilación:

```sh
cmake -DENABLE_LIBRARIES=OFF
```

En caso de problemas, tendrás que arreglártelas por tu cuenta ...

Rust requiere una conexión a Internet. Para deshabilitar la compatibilidad con Rust:

```sh
cmake -DENABLE_RUST=OFF
```

<div id="running-the-clickhouse-executable-1">
  ### Ejecutar el ejecutable de ClickHouse
</div>

Puede reemplazar la versión de producción del binario de ClickHouse instalada en su sistema por el binario de ClickHouse compilado.
Para ello, instale ClickHouse en su equipo siguiendo las instrucciones del sitio web oficial.
A continuación, ejecute:

```bash
sudo service clickhouse-server stop
sudo cp ClickHouse/build/programs/clickhouse /usr/bin/
sudo service clickhouse-server start
```

Ten en cuenta que `clickhouse-client`, `clickhouse-server` y otros son enlaces simbólicos al binario compartido `clickhouse`.

También puedes ejecutar tu binario personalizado de ClickHouse con el archivo de configuración del paquete de ClickHouse instalado en tu sistema:

```bash
sudo service clickhouse-server stop
sudo -u clickhouse ClickHouse/build/programs/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

<div id="building-on-any-linux">
  ### Compilar en cualquier sistema Linux
</div>

Instala los requisitos previos en OpenSUSE Tumbleweed:

```bash
sudo zypper install git cmake ninja clang-c++ python lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

Instala los requisitos previos en Fedora Rawhide:

```bash
sudo yum update
sudo yum --nogpg install git cmake make clang python3 ccache lld nasm yasm gawk
git clone --recursive https://github.com/ClickHouse/ClickHouse.git
mkdir build
cmake -S . -B build
cmake --build build
```

<div id="building-in-docker">
  ### Compilar con Docker
</div>

Puedes ejecutar cualquier compilación localmente en un entorno similar a CI con:

```bash
python -m ci.praktika run "BUILD_JOB_NAME"
```

donde BUILD&#95;JOB&#95;NAME es el nombre del job, tal como se muestra en el informe de CI; por ejemplo, &quot;Build (arm&#95;release)&quot;, &quot;Build (amd&#95;debug)&quot;

Este comando descarga la imagen de Docker adecuada, `clickhouse/binary-builder`, con todas las dependencias necesarias
y ejecuta dentro de ella el script de compilación: `./ci/jobs/build_clickhouse.py`

La salida de la compilación se guardará en `./ci/tmp/`.

Funciona tanto en arquitecturas AMD como ARM y no requiere dependencias adicionales, aparte de Python con el módulo `requests` disponible y Docker.