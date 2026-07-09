---
description: 'Página con detalles sobre el perfilado de asignaciones en ClickHouse'
sidebar_label: 'Perfilado de asignaciones para versiones anteriores a la 25.9'
slug: /operations/allocation-profiling-old
title: 'Perfilado de asignaciones para versiones anteriores a la 25.9'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="allocation-profiling-for-versions-before-259">
  # Perfilado de asignaciones para versiones anteriores a la 25.9
</div>

ClickHouse usa [jemalloc](https://github.com/jemalloc/jemalloc) como asignador global. Jemalloc incluye algunas herramientas para el muestreo y el perfilado de asignaciones.
Para facilitar el perfilado de asignaciones, se proporcionan comandos `SYSTEM` junto con comandos de cuatro letras (4LW) en Keeper.

<div id="sampling-allocations-and-flushing-heap-profiles">
  ## Muestreo de asignaciones y volcado de perfiles de heap
</div>

Si quieres muestrear y perfilar las asignaciones en `jemalloc`, debes iniciar ClickHouse/Keeper con la generación de perfiles habilitada mediante la variable de entorno `MALLOC_CONF`:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:true
```

`jemalloc` muestreará las asignaciones y almacenará la información internamente.

Puede indicarle a `jemalloc` que vuelque el perfil actual ejecutando:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC FLUSH PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmfp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

De forma predeterminada, el archivo del perfil de heap se generará en `/tmp/jemalloc_clickhouse._pid_._seqnum_.heap`, donde `_pid_` es el PID de ClickHouse y `_seqnum_` es el número de secuencia global del perfil de heap actual.
En Keeper, el archivo predeterminado es `/tmp/jemalloc_keeper._pid_._seqnum_.heap` y sigue las mismas reglas.

Puede definirse una ubicación distinta añadiendo la opción `prof_prefix` a la variable de entorno `MALLOC_CONF`.
Por ejemplo, si quiere generar perfiles en la carpeta `/data`, donde el prefijo del nombre de archivo será `my_current_profile`, puede ejecutar ClickHouse/Keeper con la siguiente variable de entorno:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_prefix:/data/my_current_profile
```

Al archivo generado se le añadirán el prefijo PID y el número de secuencia.

<div id="analyzing-heap-profiles">
  ## Análisis de perfiles de heap
</div>

Una vez generados los perfiles de heap, es necesario analizarlos.
Para ello, puede utilizarse la herramienta de `jemalloc` llamada [jeprof](https://github.com/jemalloc/jemalloc/blob/dev/bin/jeprof.in). Puede instalarse de varias formas:

* Usando el gestor de paquetes del sistema
* Clonando el [repositorio de jemalloc](https://github.com/jemalloc/jemalloc) y ejecutando `autogen.sh` desde la carpeta raíz. Esto proporcionará el script `jeprof` dentro de la carpeta `bin`

:::note
`jeprof` usa `addr2line` para generar stacktraces, lo que puede ser bastante lento.
Si es así, se recomienda instalar una [implementación alternativa](https://github.com/gimli-rs/addr2line) de la herramienta.

```bash
git clone https://github.com/gimli-rs/addr2line.git --depth=1 --branch=0.23.0
cd addr2line
cargo build --features bin --release
cp ./target/release/addr2line path/to/current/addr2line
```

:::

Hay muchos formatos distintos que se pueden generar a partir del perfil de heap con `jeprof`.
Se recomienda ejecutar `jeprof --help` para obtener información sobre su uso y las distintas opciones que ofrece la herramienta.

En general, el comando `jeprof` se usa de la siguiente manera:

```sh
jeprof path/to/binary path/to/heap/profile --output_format [ > output_file]
```

Si quieres comparar qué asignaciones de memoria se produjeron entre dos perfiles, puedes establecer el argumento `base`:

```sh
jeprof path/to/binary --base path/to/first/heap/profile path/to/second/heap/profile --output_format [ > output_file]
```

<div id="examples">
  ### Ejemplos
</div>

* si quieres generar un archivo de texto con cada procedimiento escrito en una línea distinta:

```sh
jeprof path/to/binary path/to/heap/profile --text > result.txt
```

* si quieres generar un archivo PDF con un grafo de llamadas:

```sh
jeprof path/to/binary path/to/heap/profile --pdf > result.pdf
```

<div id="generating-flame-graph">
  ### Generación de un flame graph
</div>

`jeprof` permite generar trazas de pila colapsadas para crear flame graphs.

Debe usar el argumento `--collapsed`:

```sh
jeprof path/to/binary path/to/heap/profile --collapsed > result.collapsed
```

Después de eso, puedes usar muchas herramientas distintas para visualizar las trazas de pila colapsadas.

La más popular es [FlameGraph](https://github.com/brendangregg/FlameGraph), que incluye un script llamado `flamegraph.pl`:

```sh
cat result.collapsed | /path/to/FlameGraph/flamegraph.pl --color=mem --title="Allocation Flame Graph" --width 2400 > result.svg
```

Otra herramienta interesante es [speedscope](https://www.speedscope.app/), que te permite analizar las trazas de pila recopiladas de forma más interactiva.

<div id="controlling-allocation-profiler-during-runtime">
  ## Control del perfilador de asignaciones en tiempo de ejecución
</div>

Si ClickHouse/Keeper se inicia con el perfilador habilitado, se admiten comandos adicionales para deshabilitar o habilitar el perfilado de asignaciones en tiempo de ejecución.
Con estos comandos, es más fácil perfilar solo intervalos específicos.

Para deshabilitar el perfilador:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC DISABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmdp | nc localhost 9181
    ```
  </TabItem>
</Tabs>

Para habilitar el perfilador:

<Tabs groupId="binary">
  <TabItem value="clickhouse" label="ClickHouse">
    ```sql
    SYSTEM JEMALLOC ENABLE PROFILE
    ```
  </TabItem>

  <TabItem value="keeper" label="Keeper">
    ```sh
    echo jmep | nc localhost 9181
    ```
  </TabItem>
</Tabs>

También es posible controlar el estado inicial del perfilador configurando la opción `prof_active`, que está habilitada de forma predeterminada.
Por ejemplo, si no desea tomar muestras de asignaciones durante el inicio, sino solo después, puede habilitar el perfilador. Puede iniciar ClickHouse/Keeper con la siguiente variable de entorno:

```sh
MALLOC_CONF=background_thread:true,prof:true,prof_active:false
```

El perfilador se puede habilitar más adelante.

<div id="additional-options-for-profiler">
  ## Opciones adicionales para el perfilador
</div>

`jemalloc` dispone de muchas opciones relacionadas con el perfilador. Se pueden controlar modificando la variable de entorno `MALLOC_CONF`.
Por ejemplo, el intervalo entre las muestras de asignación puede controlarse con `lg_prof_sample`.
Si desea generar un perfil de heap cada N bytes, puede habilitarlo con `lg_prof_interval`.

Se recomienda consultar la [página de referencia](https://jemalloc.net/jemalloc.3.html) de `jemalloc` para ver la lista completa de opciones.

<div id="other-resources">
  ## Otros recursos
</div>

ClickHouse/Keeper expone métricas relacionadas con `jemalloc` de muchas maneras diferentes.

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

<div id="prometheus">
  ### Prometheus
</div>

Todas las métricas relacionadas con `jemalloc` de `asynchronous_metrics` también se exponen a través del endpoint de Prometheus tanto en ClickHouse como en Keeper.

[Referencia](/es/operations/server-configuration-parameters/settings#prometheus)

<div id="jmst-4lw-command-in-keeper">
  ### Comando 4LW `jmst` en Keeper
</div>

Keeper es compatible con el comando 4LW `jmst`, que devuelve [estadísticas básicas del asignador de memoria](https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Basic-Allocator-Statistics):

```sh
echo jmst | nc localhost 9181
```