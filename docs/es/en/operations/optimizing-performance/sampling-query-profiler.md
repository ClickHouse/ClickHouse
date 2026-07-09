---
description: 'Documentación de la herramienta de perfilado de consultas por muestreo de ClickHouse'
sidebar_label: 'Perfilado de consultas'
sidebar_position: 54
slug: /operations/optimizing-performance/sampling-query-profiler
title: 'Perfilador de consultas por muestreo'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="sampling-query-profiler">
  # Perfilador de consultas por muestreo
</div>

ClickHouse ejecuta un perfilador por muestreo que permite analizar la ejecución de consultas.
Con este perfilador, puedes encontrar las rutinas del código fuente que se usan con mayor frecuencia durante la ejecución de una consulta.
Puedes rastrear el tiempo de CPU y el tiempo de reloj transcurrido, incluido el tiempo inactivo.

El perfilador de consultas se habilita automáticamente en ClickHouse Cloud.
La siguiente consulta de ejemplo encuentra las trazas de pila más frecuentes de una consulta perfilada, con los nombres de las funciones resueltos y las ubicaciones en el código fuente:

:::tip
Reemplaza el valor de `query_id` por el ID de la consulta que quieres perfilar.
:::

<Tabs groupId="deployment">
  <TabItem value="cloud" label="ClickHouse Cloud">
    En ClickHouse Cloud, puedes obtener el ID de la consulta haciendo clic en **&quot;...&quot;** en el extremo derecho de la barra situada encima de la tabla de resultados de la consulta (junto al selector de tabla/gráfico). Esto abre un menú contextual en el que puedes hacer clic en **&quot;Copy query ID&quot;**.

    Usa `clusterAllReplicas(default, system.trace_log)` para seleccionar datos de todos los nodos del clúster:

    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM clusterAllReplicas(default, system.trace_log)
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>

  <TabItem value="self-managed" label="Autogestionado">
    ```sql
    SELECT
        count(),
        arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'CPU' AND event_date = today()
    GROUP BY trace
    ORDER BY count() DESC
    LIMIT 10
    SETTINGS allow_introspection_functions = 1
    ```
  </TabItem>
</Tabs>

<div id="self-managed-query-profiler">
  ## Uso del perfilador de consultas en implementaciones autogestionadas
</div>

En implementaciones autogestionadas, para usar el perfilador de consultas, sigue estos pasos:

<VerticalStepper headerLevel="h3">
  ### Instalar ClickHouse con información de depuración

  Instala el paquete `clickhouse-common-static-dbg`:

  1. Sigue las instrucciones del paso [&quot;Configurar el repositorio de Debian&quot;](/es/install/debian_ubuntu#setup-the-debian-repository)
  2. Ejecuta `sudo apt-get install clickhouse-server clickhouse-client clickhouse-common-static-dbg` para instalar los archivos binarios compilados de ClickHouse con información de depuración
  3. Ejecuta `sudo service clickhouse-server start` para iniciar el servidor
  4. Ejecuta `clickhouse-client`. El servidor detectará automáticamente los símbolos de depuración de clickhouse-common-static-dbg; no necesitas hacer nada especial para habilitarlos

  ### Comprobar la configuración del servidor

  Asegúrate de que la sección [`trace_log`](../../operations/server-configuration-parameters/settings.md#trace_log) de tu [archivo de configuración del servidor](/es/operations/configuration-files) esté configurada. Está habilitada de forma predeterminada:

  ```xml
  <!-- Registro de trazas. Almacena trazas de pila recopiladas por los perfiladores de consultas.
       Consulta la configuración query_profiler_real_time_period_ns y query_profiler_cpu_time_period_ns. -->
  <trace_log>
      <database>system</database>
      <table>trace_log</table>

      <partition_by>toYYYYMM(event_date)</partition_by>
      <flush_interval_milliseconds>7500</flush_interval_milliseconds>
      <max_size_rows>1048576</max_size_rows>
      <reserved_size_rows>8192</reserved_size_rows>
      <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
      <!-- Indica si los registros deben volcarse al disco en caso de fallo -->
      <flush_on_crash>false</flush_on_crash>
      <symbolize>true</symbolize>
  </trace_log>
  ```

  Esta sección configura la tabla del sistema [trace&#95;log](/es/operations/system-tables/trace_log), que contiene los resultados del funcionamiento del perfilador.
  Recuerda que los datos de esta tabla solo son válidos mientras el servidor está en ejecución.
  Después de reiniciar el servidor, ClickHouse no limpia la tabla y todas las direcciones de memoria virtual almacenadas pueden dejar de ser válidas.

  ### Configurar los temporizadores del perfilador

  Configura las opciones [`query_profiler_cpu_time_period_ns`](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) o [`query_profiler_real_time_period_ns`](../../operations/settings/settings.md#query_profiler_real_time_period_ns).
  Ambas opciones pueden usarse simultáneamente.

  Estas opciones te permiten configurar los temporizadores del perfilador.
  Como se trata de configuraciones de sesión, puedes obtener distintas frecuencias de muestreo para todo el servidor, usuarios individuales o perfiles de usuario, para tu sesión interactiva y para cada consulta individual.

  La frecuencia de muestreo predeterminada es de una muestra por segundo, y tanto los temporizadores de CPU como los de tiempo real están habilitados.
  Esta frecuencia te permite recopilar suficiente información sobre tu clúster de ClickHouse sin afectar al rendimiento del servidor.
  Si necesitas perfilar cada consulta individual, usa una frecuencia de muestreo mayor.

  ### Analizar la tabla del sistema `trace_log`

  Para analizar la tabla del sistema `trace_log`, habilita las funciones de introspección con la opción [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions):

  ```sql
  SET allow_introspection_functions=1
  ```

  :::note
  Por motivos de seguridad, las funciones de introspección están deshabilitadas de forma predeterminada
  :::

  Usa las funciones de introspección `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` y `demangle` [funciones de introspección](../../sql-reference/functions/introspection.md) para obtener nombres de funciones y sus posiciones en el código de ClickHouse.
  Para obtener un perfil de una consulta, necesitas realizar una agregación de los datos de la tabla `trace_log`.
  Puedes agregar los datos por funciones individuales o por trazas de pila completas.

  :::tip
  Si necesitas visualizar la información de `trace_log`, prueba [flamegraph](/es/interfaces/third-party/gui#clickhouse-flamegraph) y [speedscope](https://www.speedscope.app).
  :::
</VerticalStepper>

<div id="flamegraph">
  ## Creación de gráficos de llamas con la función `flameGraph`
</div>

ClickHouse proporciona la [función de agregación `flameGraph`](/es/sql-reference/aggregate-functions/reference/flame_graph), que genera un gráfico de llamas directamente a partir de las trazas de pila almacenadas en `trace_log`.
La salida es un array de strings en un formato compatible con [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

**Sintaxis:**

```sql
flameGraph(traces, [size = 1], [ptr = 0])
```

**Argumentos:**

* `traces` — una traza de pila. [`Array(UInt64)`](/es/sql-reference/data-types/array).
* `size` — el tamaño de una asignación para el perfilado de memoria. [`Int64`](/es/sql-reference/data-types/int-uint).
* `ptr` — una dirección de asignación. [`UInt64`](/es/sql-reference/data-types/int-uint).

Cuando `ptr` es distinto de cero, `flameGraph` empareja las asignaciones (`size > 0`) y las liberaciones (`size < 0`) que tienen el mismo tamaño y puntero.
Solo se muestran las asignaciones que no se liberaron.
Las liberaciones sin coincidencia se ignoran.

<div id="cpu-flame-graph">
  ### Gráfico de llamas de CPU
</div>

:::note
Las consultas siguientes requieren que tengas instalado [flamegraph.pl](https://github.com/brendangregg/FlameGraph).

Puedes hacerlo ejecutando:

```bash
git clone https://github.com/brendangregg/FlameGraph
# Then use it as:
# ~/FlameGraph/flamegraph.pl
```

Sustituya `flamegraph.pl` en las siguientes consultas por la ruta donde se encuentra `flamegraph.pl` en su equipo
:::

```sql
SET query_profiler_cpu_time_period_ns = 10000000;
```

Ejecute la consulta y, a continuación, genere el gráfico de llamas:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(arrayReverse(trace)))
        FROM system.trace_log
        WHERE trace_type = 'CPU' AND query_id = '<query_id>'" \
    | flamegraph.pl > flame_cpu.svg
```

<div id="memory-flame-graph-all">
  ### Gráfico de llamas de memoria — todas las asignaciones de memoria
</div>

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

Ejecuta la consulta y luego genera el gráfico de llamas:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
```

<div id="memory-flame-graph-unfreed">
  ### Gráfico de llamas de memoria — asignaciones no liberadas
</div>

Esta variante empareja las asignaciones con las desasignaciones por puntero y muestra solo la memoria que no se liberó durante la consulta.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1,
    use_uncompressed_cache = 1,
    merge_tree_max_rows_to_use_cache = 100000000000,
    merge_tree_max_bytes_to_use_cache = 1000000000000;
```

Ejecute la siguiente consulta para generar el gráfico de llamas:

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM system.trace_log
        WHERE trace_type = 'MemorySample' AND query_id = '<query_id>'" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_unfreed.svg
```

<div id="memory-flame-graph-time-point">
  ### Gráfico de llamas de memoria — asignaciones activas en un momento dado
</div>

Este enfoque permite detectar el pico de uso de memoria y visualizar qué se asignó en ese momento.

```sql
SET memory_profiler_sample_probability = 1, max_untracked_memory = 1;
```

<div id="find-memory-usage-over-time">
  #### Ver el uso de memoria a lo largo del tiempo
</div>

```sql
SELECT
    event_time,
    formatReadableSize(max(s)) AS m
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
)
GROUP BY event_time
ORDER BY event_time;
```

<div id="find-time-point-maximum-memory-usage">
  #### Encuentra el instante con el mayor uso de memoria
</div>

```sql
SELECT
    argMax(event_time, s),
    max(s)
FROM (
    SELECT
        event_time,
        sum(size) OVER (ORDER BY event_time) AS s
    FROM system.trace_log
    WHERE query_id = '<query_id>' AND trace_type = 'MemorySample'
);
```

<div id="build-flame-graph">
  #### Cree un gráfico de llamas de las asignaciones de memoria activas en ese momento
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time <= '<time_point>'
            ORDER BY event_time
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
```

<div id="build-flame-graph-deallocations">
  #### Cree un gráfico de llamas de las liberaciones de memoria después de ese momento (para entender qué se liberó más tarde)
</div>

```bash
clickhouse client --allow_introspection_functions=1 \
    -q "SELECT arrayJoin(flameGraph(trace, -size, ptr))
        FROM (
            SELECT * FROM system.trace_log
            WHERE trace_type = 'MemorySample'
              AND query_id = '<query_id>'
              AND event_time > '<time_point>'
            ORDER BY event_time DESC
        )" \
    | flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
```

<div id="example">
  ## Ejemplo
</div>

El siguiente fragmento de código:

* Filtra los datos de `trace_log` por un identificador de consulta y la fecha actual.
* Agrupa por traza de pila.
* Usa funciones de introspección para obtener un informe de:
  * Los nombres de los símbolos y las funciones correspondientes en el código fuente.
  * Las ubicaciones de estas funciones en el código fuente.

```sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = '<query_id>') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```