---
description: 'Página que describe el perfilado de asignaciones en ClickHouse'
sidebar_label: 'Perfilado de asignaciones'
slug: /operations/allocation-profiling
title: 'Perfilado de asignaciones'
doc_type: 'guide'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling">
  # Perfilado de asignaciones
</div>

ClickHouse usa [jemalloc](https://github.com/jemalloc/jemalloc) como asignador global. Jemalloc incluye herramientas para el muestreo y el perfilado de asignaciones.

ClickHouse y Keeper le permiten controlar el muestreo mediante archivos de configuración, ajustes de consulta, comandos `SYSTEM` y comandos de cuatro letras (4LW) en Keeper. Hay varias formas de inspeccionar los resultados:

* Recopilar muestras en `system.trace_log` con el tipo `JemallocSample` para el análisis por consulta.
* Ver estadísticas de memoria en tiempo real y obtener perfiles del heap a través de la [interfaz web de jemalloc](#jemalloc-web-ui) integrada (26.2+).
* Consultar el perfil actual del heap directamente desde SQL usando [`system.jemalloc_profile_text`](#fetching-heap-profiles-from-sql) (26.2+).
* Volcar perfiles del heap a disco y analizarlos con [`jeprof`](#analyzing-heap-profile-files-with-jeprof).

:::note

Esta guía se aplica a las versiones 25.9+.
Para versiones anteriores, consulte [el perfilado de asignaciones para versiones anteriores a la 25.9](/es/operations/allocation-profiling-old.md).

:::

<div id="sampling-allocations">
  ## Muestreo de asignaciones
</div>

Para muestrear y perfilar las asignaciones, inicie ClickHouse/Keeper con la configuración `jemalloc_enable_global_profiler` activada:

```xml
<clickhouse>
    <jemalloc_enable_global_profiler>1</jemalloc_enable_global_profiler>
</clickhouse>
```

`jemalloc` muestreará las asignaciones de memoria y almacenará la información internamente.

También puede habilitar el muestreo por consulta con la configuración `jemalloc_enable_profiler`.

:::warning Advertencia
Como ClickHouse es una aplicación que realiza muchas asignaciones de memoria, el muestreo de jemalloc puede afectar al rendimiento.
:::

<div id="storing-jemalloc-samples-in-system-trace-log">
  ## Almacenar muestras de jemalloc en `system.trace_log`
</div>

Puede almacenar muestras de jemalloc en `system.trace_log` con el tipo `JemallocSample`.
Para habilitarlo de forma global, use la configuración `jemalloc_collect_global_profile_samples_in_trace_log`:

```xml
<clickhouse>
    <jemalloc_collect_global_profile_samples_in_trace_log>1</jemalloc_collect_global_profile_samples_in_trace_log>
</clickhouse>
```

:::warning Advertencia
Dado que ClickHouse es una aplicación que realiza muchas asignaciones de memoria, recopilar todas las muestras en system.trace&#95;log puede generar una carga elevada.
:::

También puede habilitarlo por consulta mediante la configuración `jemalloc_collect_profile_samples_in_trace_log`.

<div id="example-analyzing-memory-usage-trace-log">
  ### Ejemplo: análisis del uso de memoria de una consulta
</div>

Primero, ejecute una consulta con el profiler de jemalloc habilitado y recopile las muestras en `system.trace_log`:

```sql
SELECT *
FROM numbers(1000000)
ORDER BY number DESC
SETTINGS max_bytes_ratio_before_external_sort = 0
FORMAT `Null`
SETTINGS jemalloc_enable_profiler = 1, jemalloc_collect_profile_samples_in_trace_log = 1

Query id: 8678d8fe-62c5-48b8-b0cd-26851c62dd75

Ok.

0 rows in set. Elapsed: 0.009 sec. Processed 1.00 million rows, 8.00 MB (108.58 million rows/s., 868.61 MB/s.)
Peak memory usage: 12.65 MiB.
```

:::note
Si ClickHouse se inició con `jemalloc_enable_global_profiler`, no es necesario habilitar `jemalloc_enable_profiler`.
Lo mismo ocurre con `jemalloc_collect_global_profile_samples_in_trace_log` y `jemalloc_collect_profile_samples_in_trace_log`.
:::

Vacíe `system.trace_log`:

```sql
SYSTEM FLUSH LOGS trace_log
```

Luego, consúltalo para obtener el uso acumulado de memoria a lo largo del tiempo:

```sql
WITH per_bucket AS
(
    SELECT
        event_time_microseconds AS bucket_time,
        sum(size) AS bucket_sum
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
    GROUP BY bucket_time
)
SELECT
    bucket_time,
    sum(bucket_sum) OVER (
        ORDER BY bucket_time ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_size,
    formatReadableSize(cumulative_size) AS cumulative_size_readable
FROM per_bucket
ORDER BY bucket_time
```

Encuentra el momento en que el uso de memoria fue mayor:

```sql
SELECT
    argMax(bucket_time, cumulative_size),
    max(cumulative_size)
FROM
(
    WITH per_bucket AS
    (
        SELECT
            event_time_microseconds AS bucket_time,
            sum(size) AS bucket_sum
        FROM system.trace_log
        WHERE trace_type = 'JemallocSample'
          AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
        GROUP BY bucket_time
    )
    SELECT
        bucket_time,
        sum(bucket_sum) OVER (
            ORDER BY bucket_time ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_size,
        formatReadableSize(cumulative_size) AS cumulative_size_readable
    FROM per_bucket
    ORDER BY bucket_time
)
```

Con ese resultado, vea qué pilas de asignación fueron las más activas durante el pico:

```sql
SELECT
    concat(
        '\n',
        arrayStringConcat(
            arrayMap(
                (x, y) -> concat(x, ': ', y),
                arrayMap(x -> addressToLine(x), allocation_trace),
                arrayMap(x -> demangle(addressToSymbol(x)), allocation_trace)
            ),
            '\n'
        )
    ) AS symbolized_trace,
    sum(s) AS per_trace_sum
FROM
(
    SELECT
        ptr,
        sum(size) AS s,
        argMax(trace, event_time_microseconds) AS allocation_trace
    FROM system.trace_log
    WHERE trace_type = 'JemallocSample'
      AND query_id = '8678d8fe-62c5-48b8-b0cd-26851c62dd75'
      AND event_time_microseconds <= '2025-09-04 11:56:21.737139'
    GROUP BY ptr
    HAVING s > 0
)
GROUP BY ALL
ORDER BY per_trace_sum ASC
```

<div id="jemalloc-web-ui">
  ## web UI de jemalloc
</div>

:::note
Esta sección se aplica a las versiones 26.2+.
:::

ClickHouse proporciona una web UI integrada para ver estadísticas de memoria de jemalloc en el endpoint HTTP `/jemalloc`.
Muestra métricas de memoria en tiempo real con gráficos, incluidas `allocated`, `active`, `resident` y `mapped`, así como estadísticas por arena y por bin.
También puedes obtener perfiles de heap globales y por consulta directamente desde la UI.

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```text
    http://localhost:8123/jemalloc
    ```

    La UI del servidor incluye todas las pestañas: Summary, Allocations, Arenas, Operations, Global Profiler, Query Profiler y Raw Output.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```text
    http://localhost:9182/jemalloc
    ```

    La UI de Keeper está disponible en el puerto de control HTTP. Este puerto está **deshabilitado de forma predeterminada** y debe habilitarse explícitamente configurando `keeper_server.http_control.port` en la configuración de Keeper:

    ```xml
    <clickhouse>
        <keeper_server>
            <http_control>
                <port>9182</port>
            </http_control>
        </keeper_server>
    </clickhouse>
    ```

    Una vez habilitada, la UI ofrece las mismas visualizaciones que el servidor — Summary, Allocations, Arenas, Operations, Global Profiler y Raw Output — excepto la pestaña Query Profiler, que requiere SQL y `system.trace_log`.

    :::warning Seguridad
    El puerto de control HTTP de Keeper no tiene autenticación a nivel de aplicación. A diferencia de la UI de jemalloc del servidor ClickHouse — donde todas las consultas de datos pasan por el handler HTTP de SQL y requieren credenciales de usuario/contraseña — los API endpoints REST de Keeper no requieren autenticación. Esto es coherente con otros endpoints del control HTTP de Keeper (commands, storage, dashboard).

    Restringe el acceso a este puerto mediante controles a nivel de red: vincula Keeper a localhost, usa reglas de firewall o colócalo detrás de un proxy inverso con autenticación. Si no se configura `listen_host`, Keeper escucha únicamente en localhost de forma predeterminada.
    :::

    Keeper también expone API endpoints REST para acceso programático:

    * `GET /jemalloc/stats` — salida sin procesar de `malloc_stats_print`
    * `GET /jemalloc/status` — estado del profiling en JSON (`prof_enabled`, `prof_active`, `thread_active_init`, `lg_sample`)
    * `GET /jemalloc/profile?format={collapsed|raw}` — fuerza el volcado de un perfil de heap con simbolización server-side y devuelve collapsed stacks aptas para generar un flame graph (predeterminado) o el volcado sin procesar de jemalloc
  </TabItem>
</Tabs>

<div id="fetching-heap-profiles-from-sql">
  ## Obtención de perfiles de heap desde SQL
</div>

:::note
Esta sección se aplica a las versiones 26.2 o posteriores.
:::

La tabla del sistema `system.jemalloc_profile_text` le permite obtener y ver el perfil de heap actual de jemalloc directamente desde SQL, sin necesidad de herramientas externas ni de volcarlo primero a disco.

La tabla tiene una sola columna:

| Columna | Tipo   | Descripción                                       |
| ------- | ------ | ------------------------------------------------- |
| `line`  | String | Línea del perfil de heap de jemalloc simbolizado. |

Puede consultar la tabla directamente — no es necesario volcar previamente un perfil de heap:

```sql
SELECT * FROM system.jemalloc_profile_text
```

<div id="output-format">
  ### Formato de salida
</div>

El formato de salida se controla mediante el ajuste `jemalloc_profile_text_output_format`, que admite tres valores:

* `raw` — perfil del heap sin procesar, tal como lo genera jemalloc.
* `symbolized` — formato compatible con jeprof con símbolos de función incorporados. Como los símbolos ya están incorporados, `jeprof` puede analizar la salida sin necesidad del binario de ClickHouse.
* `collapsed` (predeterminado) — trazas de pila colapsadas compatibles con FlameGraph, una pila por línea con el recuento de bytes.

Por ejemplo, para obtener el perfil sin procesar:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'raw'
```

Para obtener el resultado simbolizado:

```sql
SELECT * FROM system.jemalloc_profile_text
SETTINGS jemalloc_profile_text_output_format = 'symbolized'
```

<div id="fetching-heap-profiles-settings">
  ### Configuración adicional
</div>

* `jemalloc_profile_text_symbolize_with_inline` (Bool, predeterminado: `true`) — Si se deben incluir frames inline al simbolizar. Desactivarlo acelera significativamente la simbolización, pero reduce la precisión, ya que las llamadas a funciones inline no aparecerán en las pilas de llamadas. Solo afecta a los formatos `symbolized` y `collapsed`.
* `jemalloc_profile_text_collapsed_use_count` (Bool, predeterminado: `false`) — Al usar el formato `collapsed`, realiza la agregación por número de asignaciones en lugar de por bytes.

<div id="example-flamegraph-from-sql">
  ### Ejemplo: generar un flame graph desde SQL
</div>

Como el formato de salida predeterminado es `collapsed`, puedes enviar la salida directamente a FlameGraph mediante una tubería:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text" | flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Para generar un flame graph según el número de asignaciones en lugar de por bytes:

```sh
clickhouse-client -q "SELECT * FROM system.jemalloc_profile_text SETTINGS jemalloc_profile_text_collapsed_use_count = 1" | flamegraph.pl --color=mem --title="Allocation Count Flame Graph" --width 2400 > result.svg
```

<div id="flushing-heap-profiles">
  ## Volcado de perfiles del heap a disco
</div>

Si necesita guardar perfiles del heap como archivos para analizarlos fuera de línea con `jeprof`, puede volcarlos a disco.

De forma predeterminada, el archivo del perfil del heap se generará en `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, donde `_pid_` es el PID de ClickHouse y `_seqnum_` es el número de secuencia global del perfil del heap actual.
En Keeper, el archivo predeterminado es `/tmp/jemalloc_keeper._pid_._seqnum_.heap` y sigue las mismas reglas.

Para volcar el perfil actual:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```

    Mostrará la ubicación del perfil volcado.
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Se puede definir una ubicación distinta agregando la opción `prof_prefix` a la variable de entorno `MALLOC_CONF`.
Por ejemplo, si quiere generar perfiles en la carpeta `/data`, donde el prefijo del nombre de archivo será `my_current_profile`, puede ejecutar ClickHouse/Keeper con la siguiente variable de entorno:

```sh
MALLOC_CONF=prof_prefix:/data/my_current_profile
```

Al archivo generado se le añadirán el prefijo PID y el número de secuencia.

<div id="analyzing-heap-profile-files-with-jeprof">
  ## Análisis de archivos de perfiles de heap con `jeprof`
</div>

Después de vaciar los perfiles de heap en disco, pueden analizarse con la herramienta de `jemalloc` llamada [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). Puede instalarse de varias maneras:

* Usando el gestor de paquetes del sistema
* Clonando el [repositorio de jemalloc](https://github.com/jemalloc/jemalloc) y ejecutando `autogen.sh` desde la carpeta raíz. Esto le proporcionará el script `jeprof` dentro de la carpeta `bin`

Hay muchos formatos de salida disponibles. Ejecute `jeprof --help` para ver la lista completa de opciones.

<div id="symbolized-heap-profiles">
  ### Perfiles del heap simbolizados
</div>

A partir de la versión 26.1+, ClickHouse genera automáticamente perfiles del heap simbolizados al ejecutar `SYSTEM JEMALLOC FLUSH PROFILE`.
El perfil simbolizado (con la extensión `.symbolized`) contiene símbolos de función integrados y puede analizarse con `jeprof` sin necesidad del binario de ClickHouse.

Por ejemplo, cuando ejecutas:

```sql
SYSTEM JEMALLOC FLUSH PROFILE
```

ClickHouse devolverá la ruta del perfil simbolizado (p. ej., `/tmp/jemalloc_clickhouse.12345.0.heap.symbolized`).

A continuación, puedes analizarlo directamente con `jeprof`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --output_format [ > output_file]
```

:::note

**No se requiere el binario**: Al usar perfiles simbolizados (archivos `.symbolized`), no necesitas proporcionar a `jeprof` la ruta del binario de ClickHouse. Esto facilita mucho el análisis de perfiles en distintas máquinas o después de actualizar el binario.

:::

Si tienes un perfil de heap antiguo no simbolizado y aún tienes acceso al binario de ClickHouse, puedes usar el enfoque tradicional:

```sh
jeprof path/to/clickhouse path/to/heap/profile --output_format [ > output_file]
```

:::note

Para los perfiles no simbolizados, `jeprof` usa `addr2line` para generar stacktraces, lo que puede resultar muy lento.
Si es así, se recomienda instalar una [implementación alternativa](https://github.com/gimli-rs/addr2line) de la herramienta.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

Como alternativa, `llvm-addr2line` funciona igual de bien (pero ten en cuenta que `llvm-objdump` no es compatible con `jeprof`)

Y luego úsalo así: `jeprof --tools addr2line:/usr/bin/llvm-addr2line,nm:/usr/bin/llvm-nm,objdump:/usr/bin/objdump,c++filt:/usr/bin/llvm-cxxfilt`

:::

Al comparar dos perfiles, puedes usar el argumento `--base`:

```sh
jeprof --base /path/to/first.heap.symbolized /path/to/second.heap.symbolized --output_format [ > output_file]
```

<div id="examples">
  ### Ejemplos
</div>

Uso de perfiles simbolizados (recomendado):

* Genere un archivo de texto con un procedimiento por línea:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --text > result.txt
```

* Genera un PDF con un grafo de llamadas:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --pdf > result.pdf
```

Uso de perfiles no simbolizados (requiere el binario):

* Genere un archivo de texto con un procedimiento por línea:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --text > result.txt
```

* Genera un archivo PDF con un grafo de llamadas:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Generar un flame graph
</div>

`jeprof` permite generar collapsed stacks para crear flame graphs.

Debes usar el argumento `--collapsed`:

```sh
jeprof /tmp/jemalloc_clickhouse.12345.0.heap.symbolized --collapsed > result.collapsed
```

O con un perfil sin simbolizar:

```sh
jeprof /path/to/clickhouse /tmp/jemalloc_clickhouse.12345.0.heap --collapsed > result.collapsed
```

Después de eso, puedes usar muchas herramientas distintas para visualizar collapsed stacks.

La más popular es [FlameGraph](https://github.com/brendangregg/FlameGraph), que incluye un script llamado `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Otra herramienta interesante es [speedscope](https://www.speedscope.app/), que te permite analizar las trazas de pila recopiladas de forma más interactiva.

<div id="additional-options-for-profiler">
  ## Opciones adicionales para el perfilador
</div>

`jemalloc` dispone de muchas opciones relacionadas con el perfilador. Se pueden controlar modificando la variable de entorno `MALLOC_CONF`.
Por ejemplo, el intervalo entre las muestras de asignación puede controlarse con `lg_prof_sample`.
Si desea volcar el perfil del heap cada N bytes, puede habilitarlo con `lg_prof_interval`.

Se recomienda consultar la [página de referencia](https://jemalloc.net/jemalloc.3.html) de `jemalloc` para obtener la lista completa de opciones.

<div id="other-resources">
  ## Otros recursos
</div>

ClickHouse/Keeper exponen métricas relacionadas con `jemalloc` de muchas maneras distintas.

:::warning Advertencia
Es importante tener en cuenta que ninguna de estas métricas está sincronizada con las demás y que los valores pueden variar.
:::

<div id="system-table-asynchronous_metrics">
  ### Tabla del sistema `asynchronous_metrics`
</div>

```sql
SELECT *
FROM system.asynchronous_metrics
WHERE metric LIKE '%jemalloc%'
FORMAT Vertical
```

[Referencia](/es/operations/system-tables/asynchronous_metrics)

<div id="system-table-jemalloc_bins">
  ### Tabla del sistema `jemalloc_bins`
</div>

Contiene información sobre las asignaciones de memoria realizadas mediante el asignador jemalloc en distintas clases de tamaño (bins), agregadas de todas las arenas.

[Referencia](/es/operations/system-tables/jemalloc_bins)

<div id="system-table-jemalloc_stats">
  ### Tabla del sistema `jemalloc_stats` (26.2+)
</div>

Devuelve la salida completa de `malloc_stats_print()` en una sola cadena. Equivale al comando `SYSTEM JEMALLOC STATS`.

```sql
SELECT * FROM system.jemalloc_stats
```

<div id="prometheus">
  ### Prometheus
</div>

Todas las métricas relacionadas con `jemalloc` de `asynchronous_metrics` también se exponen a través del endpoint de Prometheus, tanto en ClickHouse como en Keeper.

[Referencia](/es/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Comando 4LW `jmst` en Keeper
</div>

Keeper es compatible con el comando 4LW `jmst`, que devuelve [estadísticas básicas del asignador](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```