---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: clickhouse-local
---

# clickhouse-local {#clickhouse-local}

El `clickhouse-local` El programa le permite realizar un procesamiento rápido en archivos locales, sin tener que implementar y configurar el servidor ClickHouse.

Acepta datos que representan tablas y las consulta usando [Nombre de la red inalámbrica (SSID):](../../sql-reference/index.md).

`clickhouse-local` utiliza el mismo núcleo que el servidor ClickHouse, por lo que es compatible con la mayoría de las características y el mismo conjunto de formatos y motores de tabla.

Predeterminada `clickhouse-local` no tiene acceso a los datos en el mismo host, pero admite la carga de la configuración del servidor `--config-file` argumento.

!!! warning "Advertencia"
    No se recomienda cargar la configuración del servidor de producción en `clickhouse-local` Porque los datos pueden dañarse en caso de error humano.

## Uso {#usage}

Uso básico:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

Argumento:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` predeterminada.
-   `-f`, `--file` — path to data, `stdin` predeterminada.
-   `-q` `--query` — queries to execute with `;` como delimitador.
-   `-N`, `--table` — table name where to put output data, `table` predeterminada.
-   `-of`, `--format`, `--output-format` — output format, `TSV` predeterminada.
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` tala.
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

También hay argumentos para cada variable de configuración de ClickHouse que se usan más comúnmente en lugar de `--config-file`.

## Ejemplos {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

El ejemplo anterior es el mismo que:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

Ahora vamos a usuario de memoria de salida para cada usuario de Unix:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

``` text
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

[Artículo Original](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
