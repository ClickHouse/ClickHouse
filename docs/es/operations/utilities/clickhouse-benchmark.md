---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: Sistema abierto.
---

# Sistema abierto {#clickhouse-benchmark}

Se conecta a un servidor ClickHouse y envía repetidamente las consultas especificadas.

Sintaxis:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

o

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

Si desea enviar un conjunto de consultas, cree un archivo de texto y coloque cada consulta en la cadena individual de este archivo. Por ejemplo:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

Luego pase este archivo a una entrada estándar de `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## Claves {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` se envía simultáneamente. Valor predeterminado: 1.
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. Para el [modo de comparación](#clickhouse-benchmark-comparison-mode) puedes usar múltiples `-h` claves.
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [modo de comparación](#clickhouse-benchmark-comparison-mode) puedes usar múltiples `-p` claves.
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` detiene el envío de consultas cuando se alcanza el límite de tiempo especificado. Valor predeterminado: 0 (límite de tiempo desactivado).
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [modo de comparación](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` realiza el [Prueba t independiente de dos muestras para estudiantes](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) prueba para determinar si las dos distribuciones no son diferentes con el nivel de confianza seleccionado.
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` emite un informe al archivo JSON especificado.
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` las salidas acumulan rastros de excepciones.
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` en la etapa especificada. Valores posibles: `complete`, `fetch_columns`, `with_mergeable_state`. Valor predeterminado: `complete`.
-   `--help` — Shows the help message.

Si desea aplicar alguna [configuración](../../operations/settings/index.md) para consultas, páselas como una clave `--<session setting name>= SETTING_VALUE`. Por ejemplo, `--max_memory_usage=1048576`.

## Salida {#clickhouse-benchmark-output}

Predeterminada, `clickhouse-benchmark` informes para cada `--delay` intervalo.

Ejemplo del informe:

``` text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

En el informe puedes encontrar:

-   Número de consultas en el `Queries executed:` campo.

-   Cadena de estado que contiene (en orden):

    -   Punto final del servidor ClickHouse.
    -   Número de consultas procesadas.
    -   QPS: QPS: ¿Cuántas consultas realizó el servidor por segundo durante un período `--delay` argumento.
    -   RPS: ¿Cuántas filas lee el servidor por segundo durante un período `--delay` argumento.
    -   MiB/s: ¿Cuántos mebibytes servidor leído por segundo durante un período especificado en el `--delay` argumento.
    -   resultado RPS: ¿Cuántas filas colocadas por el servidor al resultado de una consulta por segundo durante un período `--delay` argumento.
    -   resultado MiB/s. ¿Cuántos mebibytes colocados por el servidor al resultado de una consulta por segundo durante un período especificado en el `--delay` argumento.

-   Percentiles de tiempo de ejecución de consultas.

## Modo de comparación {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` puede comparar el rendimiento de dos servidores ClickHouse en ejecución.

Para utilizar el modo de comparación, especifique los puntos finales de ambos servidores `--host`, `--port` claves. Las claves coinciden entre sí por posición en la lista de argumentos, la primera `--host` se empareja con la primera `--port` y así sucesivamente. `clickhouse-benchmark` establece conexiones a ambos servidores, luego envía consultas. Cada consulta dirigida a un servidor seleccionado al azar. Los resultados se muestran para cada servidor por separado.

## Ejemplo {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark -i 10
```

``` text
Loaded 1 queries.

Queries executed: 6.

localhost:9000, queries 6, QPS: 6.153, RPS: 123398340.957, MiB/s: 941.455, result RPS: 61532982.200, result MiB/s: 469.459.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.159 sec.
30.000%     0.160 sec.
40.000%     0.160 sec.
50.000%     0.162 sec.
60.000%     0.164 sec.
70.000%     0.165 sec.
80.000%     0.166 sec.
90.000%     0.166 sec.
95.000%     0.167 sec.
99.000%     0.167 sec.
99.900%     0.167 sec.
99.990%     0.167 sec.



Queries executed: 10.

localhost:9000, queries 10, QPS: 6.082, RPS: 121959604.568, MiB/s: 930.478, result RPS: 60815551.642, result MiB/s: 463.986.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.160 sec.
30.000%     0.163 sec.
40.000%     0.164 sec.
50.000%     0.165 sec.
60.000%     0.166 sec.
70.000%     0.166 sec.
80.000%     0.167 sec.
90.000%     0.167 sec.
95.000%     0.170 sec.
99.000%     0.172 sec.
99.900%     0.172 sec.
99.990%     0.172 sec.
```
