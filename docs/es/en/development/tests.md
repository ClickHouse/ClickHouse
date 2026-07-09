---
description: 'Guía para probar ClickHouse y ejecutar la suite de pruebas'
sidebar_label: 'Pruebas'
sidebar_position: 40
slug: /development/tests
title: 'Pruebas de ClickHouse'
doc_type: 'guide'
---

<div id="test-types">
  ## Tipos de pruebas
</div>

En ClickHouse existen las siguientes pruebas:

* [Pruebas funcionales](#functional-tests) - un conjunto de consultas y scripts que incluye los siguientes subconjuntos superpuestos
  * [Fast test](#running-fast-tests) - el subconjunto mínimo
  * [Pruebas sin estado](#running-stateless-tests) que no requieren cargar datos en las bases de datos
  * Pruebas secuenciales que no pueden ejecutarse en paralelo
* [Pruebas de integración](#integration-tests), ejecutadas por `pytest` en un clúster
* [Pruebas unitarias](#unit-tests)
* [Pruebas de rendimiento](#performance-tests)
* [Pruebas de compilación](#build-tests)
* [Sanitizadores](#sanitizers)
* [Fuzzers](#fuzzing)
  y algunas más; consulte las secciones siguientes.

<div id="functional-tests">
  ## Pruebas funcionales
</div>

Las pruebas funcionales son las más sencillas y cómodas de usar.
La mayoría de las funcionalidades de ClickHouse se pueden probar con pruebas funcionales, y es obligatorio usarlas para cualquier cambio en el código de ClickHouse que pueda probarse de esa forma.

Cada prueba funcional envía una o varias consultas al servidor de ClickHouse en ejecución y compara el resultado con la referencia.

Las pruebas se encuentran en el directorio `./tests/queries`.

Cada prueba puede ser de uno de estos dos tipos: `.sql` y `.sh`.

* Una prueba `.sql` es un script SQL sencillo que se envía por canalización a `clickhouse-client`.
* Una prueba `.sh` es un script que se ejecuta por sí solo.

Por lo general, las pruebas SQL son preferibles a las pruebas `.sh`.
Debe usar pruebas `.sh` solo cuando necesite probar alguna funcionalidad que no pueda ejercitarse con SQL puro, como canalizar datos de entrada a `clickhouse-client` o probar `clickhouse-local`.

:::note
Un error común al probar los tipos de datos `DateTime` y `DateTime64` es asumir que el servidor usa una zona horaria específica (por ejemplo, &quot;UTC&quot;). No es así; las zonas horarias en las ejecuciones de pruebas de CI
se aleatorizan deliberadamente. La solución alternativa más sencilla es especificar explícitamente la zona horaria de los valores de prueba; por ejemplo, `toDateTime64(val, 3, 'Europe/Amsterdam')`.
:::

<div id="running-a-test-locally">
  ### Ejecutar una prueba en local
</div>

Inicie el servidor de ClickHouse en local, escuchando en el puerto predeterminado (9000).
Para ejecutar, por ejemplo, la prueba `01428_hash_set_nan_key`, vaya a la carpeta del repositorio y ejecute el siguiente comando:

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

Los resultados de la prueba (`stderr` y `stdout`) se escriben en los archivos `01428_hash_set_nan_key.[stderr|stdout]`, que se encuentran junto a la propia prueba (para `queries/0_stateless/foo.sql`, la salida estará en `queries/0_stateless/foo.stdout`).

Consulta `tests/clickhouse-test --help` para ver todas las opciones de `clickhouse-test`.
Puedes ejecutar todas las pruebas o solo un subconjunto proporcionando un filtro para los nombres de las pruebas: `./clickhouse-test substring`.
También hay opciones para ejecutar las pruebas en paralelo o en orden aleatorio.

<div id="running-tests-on-macos">
  #### Ejecutar pruebas en macOS (Darwin)
</div>

Muchas pruebas funcionales recurren a utilidades de línea de comandos de GNU (`timeout`, `head`, `sed`, `grep`, `date`, etc.). macOS trae las variantes BSD de estas herramientas, cuyo comportamiento y cuyas opciones son diferentes (por ejemplo, BSD `head` rechaza `head -c 1G`, BSD `ps` no admite las opciones largas `--` y ni siquiera existe `timeout`). Ejecutar las pruebas con las herramientas BSD provoca fallos espurios.

Los runners de CI de macOS instalan las herramientas GNU mediante Homebrew y las sitúan por delante de las BSD en `PATH`. Reproduce lo mismo en tu entorno local:

```sh
brew install coreutils gnu-sed grep
export PATH="$(brew --prefix)/opt/coreutils/libexec/gnubin:$(brew --prefix)/opt/gnu-sed/libexec/gnubin:$(brew --prefix)/opt/grep/libexec/gnubin:$PATH"
```

`coreutils` incluye GNU `timeout`, `head`, `date` y otros; `gnu-sed` y `grep` proporcionan GNU `sed` y `grep`. Después de esto, `which timeout head sed grep` debería apuntar a las rutas `gnubin`.

<div id="running-fast-tests">
  ### Ejecutar Fast test
</div>

Puede que necesites una máquina razonablemente potente para ejecutar un subconjunto de pruebas (llamado &quot;Fast test&quot;). Lo siguiente funciona en una instancia de AWS Ubuntu amd64 `t3.2xlarge` con 100 GB de almacenamiento.

1. Instala los prerrequisitos y vuelve a iniciar sesión.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
```

2. Descarga el código fuente.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. Compila el código y ejecuta las &quot;Fast test&quot;.

```sh
python -m ci.praktika run fast
```

Deberías obtener

```sh
Failed: 0, Passed: 7394, Skipped: 1795
```

Si vas a dejar la ejecución sin supervisión, puedes usar `nohup` o `disown` para que siga ejecutándose después de que se pierda la conexión `ssh`.

<div id="running-stateless-tests">
  ### Ejecutar pruebas sin estado
</div>

Puede que necesites una máquina bastante potente para ejecutar pruebas sin estado. Las siguientes instrucciones funcionan en una instancia de AWS Ubuntu amd64 `m7i.8xlarge` con 200 GB de almacenamiento.

1. Instala los prerrequisitos y vuelve a iniciar sesión.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

2. Descarga el código fuente.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. Compila el código.

```sh
python -m ci.praktika run build_debug
cp ci/tmp/build/programs/clickhouse ci/tmp
```

4. Ejecute pruebas sin estado que se puedan ejecutar en paralelo.

```sh
python -m ci.praktika run functional
```

Deberías obtener

```sh
Failed: 0, Passed: 8497, Skipped: 103
```

Nota. Los comandos `python -m ci.praktika run` ejecutan un trabajo específico de integración continua; puedes obtener más información sobre ClickHouse CI [aquí](continuous-integration.md#running-stateless-tests).

<div id="adding-a-new-test">
  ### Añadir una nueva prueba
</div>

Para añadir una nueva prueba, primero cree un archivo `.sql` o `.sh` en el directorio `queries/0_stateless`.
A continuación, genere el archivo `.reference` correspondiente con `clickhouse-client < 12345_test.sql > 12345_test.reference` o `./12345_test.sh > ./12345_test.reference`.

Las pruebas solo deben crear, eliminar, consultar, etc., tablas en la base de datos `test`, que se crea automáticamente de antemano.
Se pueden usar tablas temporales.

Para configurar localmente el mismo entorno que en CI, instale las configuraciones de prueba (usarán una implementación simulada de ZooKeeper y ajustarán algunas opciones de configuración)

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
Las pruebas deben ser

* mínimas: crear solo las tablas, columnas y complejidad estrictamente necesarias,
* rápidas: no tardar más de unos pocos segundos (mejor aún, menos de un segundo),
* correctas y deterministas: fallar si y solo si la funcionalidad en prueba no funciona,
* aisladas/sin estado: no depender del entorno ni del tiempo,
* exhaustivas: cubrir casos límite como ceros, NULL, conjuntos vacíos y excepciones (pruebas negativas; para ello, use la sintaxis `-- { serverError xyz }` y `-- { clientError xyz }`),
* limpiar las tablas al final de la prueba (por si quedan restos),
* asegurarse de que las demás pruebas no estén comprobando lo mismo (es decir, primero use grep).
  :::

<div id="templated-tests-with-jinja">
  ### Pruebas con plantillas Jinja
</div>

Una prueba `.sql` puede escribirse como una plantilla de [Jinja2](https://jinja.palletsprojects.com/) añadiendo el sufijo `.j2` al nombre del archivo, de modo que `foo.sql` pasa a ser `foo.sql.j2`. Antes de ejecutar la prueba, `clickhouse-test` procesa la plantilla para convertirla en un script `.sql` normal y ejecuta el resultado.

Esto resulta útil cuando una prueba repite la misma consulta con pequeñas variaciones: un bucle genera las consultas a partir de una plantilla compacta, en lugar de escribir cada una a mano. Las construcciones más utilizadas son:

* `{% for ... %} ... {% endfor %}` para repetir un bloque,
* `{{ expression }}` para sustituir un valor en la salida,
* `-%}` y `{%-` para eliminar los espacios en blanco adyacentes y mantener limpio el script generado.

Por ejemplo, esta plantilla:

```sql
{% for type in ['UInt8', 'UInt16', 'UInt32'] -%}
SELECT toTypeName(0::{{ type }});
{% endfor -%}
```

se muestra como:

```sql
SELECT toTypeName(0::UInt8);
SELECT toTypeName(0::UInt16);
SELECT toTypeName(0::UInt32);
```

La salida esperada puede proporcionarse como un archivo `<name>.reference` simple que contenga los resultados completamente expandidos, o como una plantilla `<name>.reference.j2`, que `clickhouse-test` renderiza del mismo modo antes de compararla. Use la forma con plantilla cuando la salida esperada también siga un patrón repetitivo. Para ver más ejemplos, consulte los archivos `*.sql.j2` existentes en `tests/queries/0_stateless/`.

<div id="restricting-test-runs">
  ### Restringir la ejecución de pruebas
</div>

Una prueba puede tener cero o más *etiquetas* que especifican restricciones sobre los contextos en los que se ejecuta en CI.

En las pruebas `.sql`, las etiquetas se colocan en la primera línea como un comentario de SQL:

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

Para las pruebas `.sh`, las etiquetas se escriben en forma de comentario en la segunda línea:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

Lista de etiquetas disponibles:

| Nombre de la etiqueta          | Qué hace                                                                                    | Ejemplo de uso                                                                                                    |
| ------------------------------ | ------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `disabled`                     | La prueba no se ejecuta                                                                     |                                                                                                                   |
| `long`                         | El tiempo de ejecución de la prueba se amplía de 1 a 10 minutos                             |                                                                                                                   |
| `deadlock`                     | La prueba se ejecuta en un bucle durante mucho tiempo                                       |                                                                                                                   |
| `race`                         | Igual que `deadlock`. Se prefiere `deadlock`                                                |                                                                                                                   |
| `shard`                        | El servidor debe escuchar en `127.0.0.*`                                                    |                                                                                                                   |
| `distributed`                  | Igual que `shard`. Se prefiere `shard`                                                      |                                                                                                                   |
| `global`                       | Igual que `shard`. Se prefiere `shard`                                                      |                                                                                                                   |
| `zookeeper`                    | La prueba requiere ZooKeeper o ClickHouse Keeper para ejecutarse                            | La prueba usa `ReplicatedMergeTree`                                                                               |
| `replica`                      | Igual que `zookeeper`. Se prefiere `zookeeper`                                              |                                                                                                                   |
| `no-fasttest`                  | La prueba no se ejecuta en [Fast test](#test-types)                                         | La prueba usa el motor de tabla `MySQL`, que está deshabilitado en Fast test                                      |
| `fasttest-only`                | La prueba solo se ejecuta en [Fast test](#test-types)                                       |                                                                                                                   |
| `no-[asan, tsan, msan, ubsan]` | Deshabilita las pruebas en compilaciones con [sanitizadores](#sanitizers)                   | La prueba se ejecuta con QEMU, que no funciona con sanitizadores                                                  |
| `no-replicated-database`       | Deshabilita la prueba cuando la base de datos predeterminada usa `ReplicatedDatabaseEngine` |                                                                                                                   |
| `no-ordinary-database`         | Deshabilita la prueba cuando el motor de base de datos predeterminado es `Ordinary`         |                                                                                                                   |
| `no-parallel`                  | Deshabilita la ejecución en paralelo de otras pruebas con esta                              | La prueba lee de tablas `system` y los invariantes pueden romperse                                                |
| `no-parallel-replicas`         | Deshabilita la prueba cuando las réplicas paralelas están habilitadas                       |                                                                                                                   |
| `no-debug`                     | Deshabilita las pruebas en compilaciones Debug                                              |                                                                                                                   |
| `no-release`                   | Deshabilita las pruebas en compilaciones Release                                            |                                                                                                                   |
| `no-darwin`                    | Deshabilita la prueba en macOS (Darwin)                                                     | La prueba depende de funciones específicas de Linux, como las consultas distribuidas, `procfs` o el servidor HTTP |

También se admiten las siguientes opciones: `no-polymorphic-parts`, `no-random-settings`, `no-random-merge-tree-settings`, `no-backward-compatibility-check`, `no-cpu-x86_64`, `no-cpu-aarch64`, `no-cpu-ppc64le`, `no-s3-storage`.

Además de la configuración anterior, puede usar indicadores `USE_*` de `system.build_options` para definir el uso de funcionalidades concretas de ClickHouse.
Por ejemplo, si su prueba usa una tabla MySQL, debe añadir la etiqueta `use-mysql`.

<div id="specifying-limits-for-random-settings">
  ### Especificación de límites para ajustes aleatorios
</div>

Una prueba puede especificar valores mínimos y máximos permitidos para los ajustes que pueden aleatorizarse durante la ejecución de la prueba.

En las pruebas `.sh`, los límites se escriben como un comentario en la línea junto a las etiquetas o en la segunda línea si no se especifican etiquetas:

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

Para las pruebas `.sql`, las etiquetas se colocan como un comentario de SQL en la línea de las etiquetas o en la primera línea:

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

Si necesita especificar solo un límite, puede usar `None` para el segundo.

<div id="choosing-the-test-name">
  ### Cómo elegir el nombre de la prueba
</div>

El nombre de la prueba comienza con un prefijo de cinco dígitos seguido de un nombre descriptivo, como `00422_hash_function_constexpr.sql`.
Para elegir el prefijo, busque el mayor prefijo que ya exista en el directorio y súmele uno.

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

Mientras tanto, puede que se añadan otras pruebas con el mismo prefijo numérico, pero no pasa nada ni ocasiona ningún problema; no tendrás que cambiarlo más adelante.

<div id="checking-for-an-error-that-must-occur">
  ### Comprobar que se produzca un error
</div>

A veces conviene comprobar que una consulta incorrecta provoque un error del servidor. Admitimos anotaciones especiales para esto en las pruebas SQL, con el siguiente formato:

```sql
SELECT x; -- { serverError 49 }
```

Esta prueba garantiza que el server devuelva un error con el código 49 por la columna desconocida `x`.
Si no se produce ningún error, o si el error es diferente, la prueba fallará.
Si quieres asegurarte de que se produzca un error del lado del client, usa en su lugar la anotación `clientError`.

No compruebes una redacción concreta del mensaje de error; puede cambiar en el futuro y hacer que la prueba falle innecesariamente.
Comprueba solo el código de error.
Si el código de error existente no es lo bastante preciso para tus necesidades, considera añadir uno nuevo.

<div id="testing-a-distributed-query">
  ### Probar una consulta distribuida
</div>

Si quieres usar consultas distribuidas en pruebas funcionales, puedes usar la función de tabla `remote` con direcciones `127.0.0.{1..2}` para que el servidor se consulte a sí mismo; o puedes usar clústeres de prueba predefinidos en el archivo de configuración del servidor, como `test_shard_localhost`.
Recuerda añadir las palabras `shard` o `distributed` al nombre de la prueba para que se ejecute en CI con la configuración correcta, en la que el servidor está preparado para admitir consultas distribuidas.

<div id="working-with-temporary-files">
  ### Trabajar con archivos temporales
</div>

A veces, en un test de shell, puede que necesites crear un archivo sobre la marcha para trabajar con él.
Ten en cuenta que algunas comprobaciones de CI ejecutan tests en paralelo, por lo que, si creas o eliminas un archivo temporal en tu script sin un nombre único, algunas comprobaciones de CI, como Flaky, pueden fallar.
Para evitarlo, debes usar la variable de entorno `$CLICKHOUSE_TEST_UNIQUE_NAME` para asignar a los archivos temporales un nombre único para el test en ejecución.
De ese modo, puedes asegurarte de que el archivo que creas durante la preparación o eliminas durante la limpieza es el que usa únicamente ese test, y no otro test que se esté ejecutando en paralelo.

<div id="known-bugs">
  ## Errores conocidos
</div>

Si conocemos errores que pueden reproducirse fácilmente con pruebas funcionales, colocamos pruebas funcionales preparadas en el directorio `tests/queries/bugs`.
Estas pruebas se moverán a `tests/queries/0_stateless` cuando se hayan corregido los errores.

<div id="integration-tests">
  ## Pruebas de integración
</div>

Las pruebas de integración permiten probar ClickHouse en una configuración en clúster y la interacción de ClickHouse con otros servidores como MySQL, Postgres y MongoDB.
Son útiles para emular particiones de red, pérdida de paquetes, etc.
Estas pruebas se ejecutan en Docker y crean varios contenedores con distinto software.

Consulta `tests/integration/README.md` para ver cómo ejecutar estas pruebas.

Ten en cuenta que no se prueba la integración de ClickHouse con drivers de terceros.
Además, actualmente no tenemos pruebas de integración con nuestros drivers JDBC y ODBC.

<div id="unit-tests">
  ## Pruebas unitarias
</div>

Las pruebas unitarias son útiles cuando se quiere probar no ClickHouse en su conjunto, sino una sola biblioteca o clase aislada.
Puedes habilitar o deshabilitar la compilación de las pruebas con la opción de CMake `ENABLE_TESTS`.
Las pruebas unitarias (y otros programas de prueba) se encuentran en los subdirectorios `tests` repartidos por el código.
Para ejecutar las pruebas unitarias, escribe `ninja test`.
Algunas pruebas usan `gtest`, pero otras son simplemente programas que devuelven un código de salida distinto de cero cuando una prueba falla.

No es necesario tener pruebas unitarias si el código ya está cubierto por pruebas funcionales (y las pruebas funcionales suelen ser mucho más fáciles de usar).

Puedes ejecutar comprobaciones individuales de `gtest` llamando directamente al ejecutable; por ejemplo:

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

<div id="performance-tests">
  ## Pruebas de rendimiento
</div>

Las pruebas de rendimiento permiten medir y comparar el rendimiento de alguna parte aislada de ClickHouse mediante consultas sintéticas.
Las pruebas de rendimiento se encuentran en `tests/performance/`.
Cada prueba está representada por un archivo `.xml` con una descripción del caso de prueba.
Las pruebas se ejecutan con la herramienta `docker/test/performance-comparison`. Consulte el archivo README para ver cómo invocarla.

Cada prueba ejecuta una o varias consultas (posiblemente con combinaciones de parámetros) en un bucle.

Si desea mejorar el rendimiento de ClickHouse en algún escenario, y si las mejoras pueden observarse en consultas simples, es muy recomendable escribir una prueba de rendimiento.
También se recomienda escribir pruebas de rendimiento cuando agregue o modifique funciones SQL relativamente aisladas y no demasiado complejas.
Siempre tiene sentido usar `perf top` u otras herramientas de `perf` durante sus pruebas.

<div id="test-tools-and-scripts">
  ## Herramientas y scripts de prueba
</div>

Algunos programas del directorio `tests` no son pruebas propiamente dichas, sino herramientas de prueba.
Por ejemplo, para `Lexer` hay una herramienta, `src/Parsers/tests/lexer`, que simplemente tokeniza stdin y escribe el resultado coloreado en stdout.
Puede usar este tipo de herramientas como ejemplos de código y para explorar y realizar pruebas manuales.

<div id="miscellaneous-tests">
  ## Pruebas diversas
</div>

Hay pruebas para modelos de aprendizaje automático en `tests/external_models`.
Estas pruebas no se actualizan y deben transferirse a las pruebas de integración.

Hay una prueba independiente para inserciones con cuórum.
Esta prueba ejecuta un clúster de ClickHouse en servidores separados y emula varios casos de fallo: partición de red, pérdida de paquetes (entre nodos de ClickHouse, entre ClickHouse y ZooKeeper, entre el servidor de ClickHouse y el client, etc.), `kill -9`, `kill -STOP` y `kill -CONT`, como [Jepsen](https://aphyr.com/tags/Jepsen). Luego, la prueba comprueba que todas las inserciones confirmadas se hayan escrito y que ninguna de las inserciones rechazadas se haya escrito.

<div id="manual-testing">
  ## Pruebas manuales
</div>

Cuando desarrolles una nueva funcionalidad, también es razonable probarla manualmente.
Puedes hacerlo siguiendo estos pasos:

Compila ClickHouse. Ejecuta ClickHouse desde la terminal: cambia al directorio `programs/clickhouse-server` y ejecútalo con `./clickhouse-server`. De forma predeterminada, usará la configuración (`config.xml`, `users.xml` y los archivos de los directorios `config.d` y `users.d`) del directorio actual. Para conectarte al servidor de ClickHouse, ejecuta `programs/clickhouse-client/clickhouse-client`.

Ten en cuenta que todas las herramientas de clickhouse (server, client, etc.) son simplemente enlaces simbólicos a un único binario llamado `clickhouse`.
Puedes encontrar este binario en `programs/clickhouse`.
Todas las herramientas también pueden invocarse como `clickhouse tool` en lugar de `clickhouse-tool`.

Como alternativa, puedes instalar el paquete de ClickHouse: ya sea una release estable del repositorio de ClickHouse, o bien puedes compilar el paquete tú mismo con `./release` en la raíz del código fuente de ClickHouse.
Luego inicia el servidor con `sudo clickhouse start` (o `stop` para detener el servidor).
Busca los logs en `/etc/clickhouse-server/clickhouse-server.log`.

Cuando ClickHouse ya esté instalado en tu sistema, puedes compilar un nuevo binario `clickhouse` y reemplazar el binario existente:

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

También puede detener el clickhouse-server del sistema y ejecutar el suyo propio con la misma configuración, pero con los registros en la terminal:

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Ejemplo con gdb:

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Si `clickhouse-server` ya se está ejecutando y no desea detenerlo, puede cambiar los números de puerto en su `config.xml` (o sobrescribirlos en un archivo del directorio `config.d`), indicar una ruta de datos adecuada y ejecutarlo.

El binario `clickhouse` casi no tiene dependencias y funciona en una amplia variedad de distribuciones de Linux.
Para probar rápidamente sus cambios en un servidor, puede simplemente copiar con `scp` su binario `clickhouse` recién compilado al servidor y luego ejecutarlo como en los ejemplos anteriores.

<div id="build-tests">
  ## Pruebas de compilación
</div>

Las pruebas de compilación permiten comprobar que la compilación no falle en varias configuraciones alternativas y en algunos sistemas distintos.
Estas pruebas también están automatizadas.

Ejemplos:

* compilación cruzada para Darwin x86&#95;64 (macOS)
* compilación cruzada para FreeBSD x86&#95;64
* compilación cruzada para Linux AArch64
* compilación en Ubuntu con bibliotecas de paquetes del sistema (desaconsejado)
* compilación con enlazado compartido de bibliotecas (desaconsejado)

Por ejemplo, compilar con paquetes del sistema es una mala práctica, porque no podemos garantizar qué versión exacta de los paquetes tendrá un sistema.
Pero esto es realmente necesario para los mantenedores de Debian.
Por esta razón, al menos tenemos que dar soporte a esta variante de compilación.
Otro ejemplo: el enlazado compartido es una fuente habitual de problemas, pero es necesario para algunos entusiastas.

Aunque no podemos ejecutar todas las pruebas en todas las variantes de compilación, queremos comprobar al menos que las distintas variantes de compilación no fallen.
Para este propósito usamos pruebas de compilación.

También comprobamos que no haya unidades de traducción demasiado grandes para compilarse o que requieran demasiada RAM.

También comprobamos que no haya marcos de pila demasiado grandes.

<div id="testing-for-protocol-compatibility">
  ## Pruebas de compatibilidad del protocolo
</div>

Cuando ampliamos el protocolo de red de ClickHouse, comprobamos manualmente que el `clickhouse-client` antiguo funcione con el `clickhouse-server` nuevo y que el `clickhouse-client` nuevo funcione con el `clickhouse-server` antiguo (simplemente ejecutando los binarios de los paquetes correspondientes).

También probamos automáticamente algunos casos mediante pruebas de integración:

* si los datos escritos por una versión antigua de ClickHouse pueden leerse correctamente con la nueva versión;
* si las consultas distribuidas funcionan en un clúster con distintas versiones de ClickHouse.

<div id="help-from-the-compiler">
  ## Ayuda del compilador
</div>

El código principal de ClickHouse (ubicado en el directorio `src`) se compila con `-Wall -Wextra -Werror` y con algunas advertencias adicionales habilitadas.
Sin embargo, estas opciones no están habilitadas para las bibliotecas de terceros.

Clang tiene aún más advertencias útiles: puedes buscarlas con `-Weverything` y elegir alguna para la compilación por defecto.

Siempre usamos Clang para compilar ClickHouse, tanto en desarrollo como en producción.
Puedes compilar en tu propia máquina en modo de depuración (para ahorrar batería de tu portátil), pero ten en cuenta que el compilador puede generar más advertencias con `-O3` gracias a un mejor flujo de control y al análisis interprocedimental.
Al compilar con Clang en modo de depuración, se usa la versión de depuración de `libc++`, lo que permite detectar más errores en tiempo de ejecución.

<div id="sanitizers">
  ## Sanitizadores
</div>

:::note
Si el proceso (ClickHouse server o client) se bloquea al iniciarse cuando lo ejecutas localmente, puede que necesites desactivar la aleatorización del espacio de direcciones: `sudo sysctl kernel.randomize_va_space=0`
:::

<div id="address-sanitizer">
  ### Sanitizador de direcciones
</div>

Ejecutamos pruebas funcionales, de integración, de estrés y unitarias con ASan en cada commit.

<div id="thread-sanitizer">
  ### Sanitizador de hilos
</div>

Ejecutamos pruebas funcionales, de integración, de estrés y unitarias bajo TSan en cada commit.

<div id="memory-sanitizer">
  ### Sanitizador de memoria
</div>

Ejecutamos pruebas funcionales, de integración, de estrés y unitarias con MSan en cada commit.

<div id="undefined-behaviour-sanitizer">
  ### Sanitizador de comportamiento indefinido
</div>

Ejecutamos pruebas funcionales, de integración, de estrés y unitarias con UBSan en cada commit.
El código de algunas bibliotecas de terceros no se analiza con el sanitizador de UB.

<div id="valgrind-memcheck">
  ### Valgrind (memcheck)
</div>

Antes ejecutábamos las pruebas funcionales con Valgrind durante la noche, pero ya no se hace.
Lleva varias horas.
Actualmente hay un falso positivo conocido en la biblioteca `re2`; consulta [este artículo](https://research.swtch.com/sparse).

<div id="fuzzing">
  ## Fuzzing
</div>

El fuzzing de ClickHouse se implementa tanto con [libFuzzer](https://llvm.org/docs/LibFuzzer.html) como con consultas SQL aleatorias.
Todas las pruebas de fuzzing deben realizarse con sanitizadores (Address y Undefined).

LibFuzzer se utiliza para realizar pruebas de fuzzing aisladas del código de bibliotecas.
Los fuzzers se implementan como parte del código de prueba y tienen el sufijo &quot;&#95;fuzzer&quot; en el nombre.
Puede encontrar un ejemplo de fuzzer en `src/Parsers/fuzzers/lexer_fuzzer.cpp`.
Las configuraciones, los diccionarios y el corpus específicos de LibFuzzer se almacenan en `tests/fuzz`.
Le recomendamos escribir pruebas de fuzzing para cada funcionalidad que procese entradas de usuario.

Los fuzzers no se compilan de forma predeterminada.
Para compilar los fuzzers, deben establecerse las opciones `-DENABLE_FUZZING=1` y `-DENABLE_TESTS=1`.
Recomendamos deshabilitar Jemalloc al compilar los fuzzers.
La configuración utilizada para integrar el fuzzing de ClickHouse con
Google OSS-Fuzz puede encontrarse en `docker/fuzz`.

También usamos una prueba de fuzzing sencilla para generar consultas SQL aleatorias y comprobar que el servidor no falle al ejecutarlas.
Puede encontrarla en `00746_sql_fuzzy.pl`.
Esta prueba debe ejecutarse de forma continua (durante la noche o más tiempo).

También usamos un fuzzer de consultas sofisticado basado en AST, capaz de encontrar una gran cantidad de casos límite.
Realiza permutaciones y sustituciones aleatorias en el AST de las consultas.
Recuerda nodos del AST de pruebas anteriores para usarlos en el fuzzing de pruebas posteriores, mientras las procesa en orden aleatorio.
Puede obtener más información sobre este fuzzer en [este artículo del blog](https://clickhouse.com/blog/fuzzing-click-house).

<div id="stress-test">
  ## Prueba de estrés
</div>

Las pruebas de estrés son otro tipo de fuzzing.
Consiste en ejecutar todas las pruebas funcionales en paralelo, en orden aleatorio, con un solo servidor.
No se comprueban los resultados de las pruebas.

Se comprueba que:

* el servidor no se bloquee y que no se disparen trampas de Debug ni del sanitizador;
* no haya interbloqueos;
* la estructura de la base de datos sea consistente;
* el servidor pueda detenerse correctamente después de la prueba y volver a iniciarse sin excepciones.

Hay cinco variantes (Debug, ASan, TSan, MSan, UBSan).

<div id="thread-fuzzer">
  ## Thread fuzzer
</div>

Thread Fuzzer (por favor, no lo confunda con Thread Sanitizer) es otro tipo de fuzzing que permite alterar aleatoriamente el orden de ejecución de los hilos.
Ayuda a encontrar aún más casos especiales.

<div id="security-audit">
  ## Auditoría de seguridad
</div>

Nuestro equipo de seguridad realizó una revisión general básica de las capacidades de ClickHouse desde el punto de vista de la seguridad.

<div id="static-analyzers">
  ## Analizadores estáticos
</div>

Ejecutamos `clang-tidy` en cada commit.
Las comprobaciones de `clang-static-analyzer` también están activadas.
`clang-tidy` también se usa para algunas comprobaciones de estilo.

Hemos evaluado `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`, `CodeQL`.
Encontrará instrucciones de uso en el directorio `tests/instructions/`.

Si usa `CLion` como IDE, puede aprovechar de inmediato algunas comprobaciones de `clang-tidy`.

También usamos `shellcheck` para el análisis estático de scripts de shell.

<div id="hardening">
  ## Refuerzo de seguridad
</div>

En la compilación de depuración usamos un asignador de memoria personalizado que aplica ASLR a las asignaciones a nivel de usuario.

También protegemos manualmente las regiones de memoria que se espera que queden en modo readonly después de la asignación.

En la compilación de depuración también incorporamos una personalización de libc que garantiza que no se llamen funciones &quot;perjudiciales&quot; (obsoletas, inseguras, no seguras para subprocesos).

Las aserciones de depuración se usan ampliamente.

En la compilación de depuración, si se lanza una excepción con código &quot;logical error&quot; (lo que implica un bug), el programa finaliza de forma prematura.
Esto permite usar excepciones en la compilación release, pero hacer que se comporte como una aserción en la compilación de depuración.

Se usa una versión de depuración de jemalloc para las compilaciones de depuración.
Se usa una versión de depuración de libc++ para las compilaciones de depuración.

<div id="runtime-integrity-checks">
  ## Comprobaciones de integridad en tiempo de ejecución
</div>

Los datos almacenados en disco llevan suma de comprobación.
Los datos de las tablas MergeTree llevan suma de comprobación de tres formas simultáneamente* (bloques de datos comprimidos, bloques de datos sin comprimir y la suma de comprobación total de todos los bloques).
Los datos transferidos por la red entre client y server, o entre servidores, también llevan suma de comprobación.
La replicación garantiza datos idénticos bit a bit en las réplicas.

Esto es necesario para protegerse frente a fallos de hardware (degradación de bits en los medios de almacenamiento, cambios de bit en la RAM del server, cambios de bit en la RAM del controlador de red, cambios de bit en la RAM del conmutador de red, cambios de bit en la RAM del client, cambios de bit on the wire).
Tenga en cuenta que los cambios de bit son habituales y pueden producirse incluso con RAM ECC y con sumas de comprobación TCP (si llega a ejecutar miles de servidores que procesan petabytes de datos al día).
[Vea el video (ruso)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

ClickHouse proporciona diagnósticos que ayudarán a los ingenieros de operaciones a detectar hardware defectuoso.

* y no es lento.

<div id="code-style">
  ## Estilo de código
</div>

Las reglas de estilo de código se describen [aquí](style.md).

Para comprobar algunas infracciones de estilo comunes, puede usar el script `utils/check-style`.

Para aplicar el estilo correcto a su código, puede usar `clang-format`.
El archivo `.clang-format` se encuentra en la raíz del código fuente.
En su mayor parte, se ajusta a nuestro estilo de código actual.
Pero no se recomienda aplicar `clang-format` a archivos existentes porque empeora el formato.
Puede usar la herramienta `clang-format-diff`, que puede encontrar en el repositorio de código fuente de clang.

Como alternativa, puede probar la herramienta `uncrustify` para reformatear su código.
La configuración está en `uncrustify.cfg`, en la raíz del código fuente.
Está menos probada que `clang-format`.

`CLion` tiene su propio formateador de código, que debe ajustarse a nuestro estilo de código.

<div id="test-coverage">
  ## Cobertura de pruebas
</div>

También hacemos un seguimiento de la cobertura de pruebas, pero solo de las pruebas funcionales y solo para clickhouse-server.
Se lleva a cabo a diario.

<div id="tests-for-tests">
  ## Pruebas para las pruebas
</div>

Hay una comprobación automatizada para detectar pruebas inestables.
Ejecuta todas las pruebas nuevas 100 veces (para las pruebas funcionales) o 10 veces (para las pruebas de integración).
Si la prueba falla хотя sea una sola vez, se considera inestable.

<div id="test-automation">
  ## Automatización de pruebas
</div>

Ejecutamos pruebas con [GitHub Actions](https://github.com/features/actions).

Los jobs de compilación y las pruebas se ejecutan en Sandbox para cada commit.
Los paquetes generados y los resultados de las pruebas se publican en GitHub y pueden descargarse mediante enlaces directos.
Los artefactos se conservan durante varios meses.
Cuando envías un pull request en GitHub, lo etiquetamos como &quot;can be tested&quot; y nuestro sistema de CI compilará paquetes de ClickHouse (release, debug, con sanitizador de direcciones, etc.) para ti.