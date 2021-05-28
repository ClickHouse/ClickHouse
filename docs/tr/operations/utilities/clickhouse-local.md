---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: clickhouse-yerel
---

# clickhouse-yerel {#clickhouse-local}

Bu `clickhouse-local` program, ClickHouse sunucusunu dağıtmak ve yapılandırmak zorunda kalmadan yerel dosyalar üzerinde hızlı işlem yapmanızı sağlar.

Tabloları temsil eden verileri kabul eder ve bunları kullanarak sorgular [ClickHouse SQL lehçesi](../../sql-reference/index.md).

`clickhouse-local` ClickHouse server ile aynı çekirdeği kullanır, bu nedenle özelliklerin çoğunu ve aynı format ve tablo motorlarını destekler.

Varsayılan olarak `clickhouse-local` aynı ana bilgisayarda verilere erişimi yok, ancak kullanarak yükleme sunucu yapılandırmasını destekler `--config-file` tartışma.

!!! warning "Uyarıcı"
    İçine üretim sunucusu yapılandırmasını yüklemek için tavsiye edilmez `clickhouse-local` çünkü insan hatası durumunda veriler zarar görebilir.

## Kullanma {#usage}

Temel kullanım:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

Değişkenler:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` varsayılan olarak.
-   `-f`, `--file` — path to data, `stdin` varsayılan olarak.
-   `-q` `--query` — queries to execute with `;` delimeter olarak.
-   `-N`, `--table` — table name where to put output data, `table` varsayılan olarak.
-   `-of`, `--format`, `--output-format` — output format, `TSV` varsayılan olarak.
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` günlük.
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

Ayrıca, bunun yerine daha yaygın olarak kullanılan her ClickHouse yapılandırma değişkeni için argümanlar vardır `--config-file`.

## Örnekler {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

Önceki örnek aynıdır:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

Şimdi her Unix kullanıcısı için bellek kullanıcısını çıkaralım:

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

[Orijinal makale](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
