---
toc_priority: 6
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

Этот движок обеспечивает интеграцию с экосистемой [Apache Hadoop](https://ru.wikipedia.org/wiki/Hadoop), позволяя управлять данными в HDFS посредством ClickHouse. Данный движок похож на движки [File](../special/file.md#table_engines-file) и [URL](../special/url.md#table_engines-url), но предоставляет возможности, характерные для Hadoop.

## Использование движка {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

**Параметры движка**

В параметр `URI` нужно передавать полный URI файла в HDFS. Часть URI с путем файла может содержать шаблоны. В этом случае таблица может использоваться только для чтения.
Параметр `format` должен быть таким, который ClickHouse может использовать и в запросах `INSERT`, и в запросах `SELECT`. Полный список поддерживаемых форматов смотрите в разделе [Форматы](../../../interfaces/formats.md#formats).


**Пример:**

**1.** Создадим на сервере таблицу `hdfs_engine_table`:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Заполним файл:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Запросим данные:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Детали реализации {#implementation-details}

-   Поддерживается многопоточное чтение и запись.
-   Поддерживается репликация без копирования данных ([zero-copy](../../../operations/storing-data.md#zero-copy)).  
-   Не поддерживается:
    -   использование операций `ALTER` и `SELECT...SAMPLE`;
    -   индексы.

**Шаблоны в пути**

Шаблоны могут содержаться в нескольких компонентах пути. Обрабатываются только существующие файлы, название которых целиком удовлетворяет шаблону (не только суффиксом или префиксом).

-   `*` — Заменяет любое количество любых символов кроме `/`, включая отсутствие символов.
-   `?` — Заменяет ровно один любой символ.
-   `{some_string,another_string,yet_another_one}` — Заменяет любую из строк `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Заменяет любое число в интервале от `N` до `M` включительно (может содержать ведущие нули).

Конструкция с `{}` аналогична табличной функции [remote](../../../engines/table-engines/integrations/hdfs.md).

**Пример**

1.  Предположим, у нас есть несколько файлов со следующими URI в HDFS:

    -  'hdfs://hdfs1:9000/some_dir/some_file_1'
    -  'hdfs://hdfs1:9000/some_dir/some_file_2'
    -  'hdfs://hdfs1:9000/some_dir/some_file_3'
    -  'hdfs://hdfs1:9000/another_dir/some_file_1'
    -  'hdfs://hdfs1:9000/another_dir/some_file_2'
    -  'hdfs://hdfs1:9000/another_dir/some_file_3'

1.  Есть несколько возможностей создать таблицу, состояющую из этих шести файлов:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Другой способ:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Таблица, состоящая из всех файлов в обеих директориях (все файлы должны удовлетворять формату и схеме, указанной в запросе):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "Warning"
    Если список файлов содержит числовые интервалы с ведущими нулями, используйте конструкцию с фигурными скобочками для каждой цифры или используйте `?`.

**Example**

Создадим таблицу с именами `file000`, `file001`, … , `file999`:

``` sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```
## Конфигурация {#configuration}

Похоже на GraphiteMergeTree, движок HDFS поддерживает расширенную конфигурацию с использованием файла конфигурации ClickHouse. Есть два раздела конфигурации которые вы можете использовать: глобальный (`hdfs`) и на уровне пользователя (`hdfs_*`). Глобальные настройки применяются первыми, и затем применяется конфигурация уровня пользователя (если она указана).

``` xml
  <!-- Глобальные настройки для движка HDFS -->
  <hdfs>
	<hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
	<hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
	<hadoop_security_authentication>kerberos</hadoop_security_authentication>
  </hdfs>

  <!-- Конфигурация специфичная для пользователя "root" -->
  <hdfs_root>
	<hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  </hdfs_root>
```

### Параметры конфигурации {#configuration-options}

#### Поддерживаемые из libhdfs3 {#supported-by-libhdfs3}


| **параметр**                                          | **по умолчанию**        |
| -                                                     | -                       |
| rpc\_client\_connect\_tcpnodelay                      | true                    |
| dfs\_client\_read\_shortcircuit                       | true                    |
| output\_replace-datanode-on-failure                   | true                    |
| input\_notretry-another-node                          | false                   |
| input\_localread\_mappedfile                          | true                    |
| dfs\_client\_use\_legacy\_blockreader\_local          | false                   |
| rpc\_client\_ping\_interval                           | 10  * 1000              |
| rpc\_client\_connect\_timeout                         | 600 * 1000              |
| rpc\_client\_read\_timeout                            | 3600 * 1000             |
| rpc\_client\_write\_timeout                           | 3600 * 1000             |
| rpc\_client\_socekt\_linger\_timeout                  | -1                      |
| rpc\_client\_connect\_retry                           | 10                      |
| rpc\_client\_timeout                                  | 3600 * 1000             |
| dfs\_default\_replica                                 | 3                       |
| input\_connect\_timeout                               | 600 * 1000              |
| input\_read\_timeout                                  | 3600 * 1000             |
| input\_write\_timeout                                 | 3600 * 1000             |
| input\_localread\_default\_buffersize                 | 1 * 1024 * 1024         |
| dfs\_prefetchsize                                     | 10                      |
| input\_read\_getblockinfo\_retry                      | 3                       |
| input\_localread\_blockinfo\_cachesize                | 1000                    |
| input\_read\_max\_retry                               | 60                      |
| output\_default\_chunksize                            | 512                     |
| output\_default\_packetsize                           | 64 * 1024               |
| output\_default\_write\_retry                         | 10                      |
| output\_connect\_timeout                              | 600 * 1000              |
| output\_read\_timeout                                 | 3600 * 1000             |
| output\_write\_timeout                                | 3600 * 1000             |
| output\_close\_timeout                                | 3600 * 1000             |
| output\_packetpool\_size                              | 1024                    |
| output\_heeartbeat\_interval                          | 10 * 1000               |
| dfs\_client\_failover\_max\_attempts                  | 15                      |
| dfs\_client\_read\_shortcircuit\_streams\_cache\_size | 256                     |
| dfs\_client\_socketcache\_expiryMsec                  | 3000                    |
| dfs\_client\_socketcache\_capacity                    | 16                      |
| dfs\_default\_blocksize                               | 64 * 1024 * 1024        |
| dfs\_default\_uri                                     | "hdfs://localhost:9000" |
| hadoop\_security\_authentication                      | "simple"                |
| hadoop\_security\_kerberos\_ticket\_cache\_path       | ""                      |
| dfs\_client\_log\_severity                            | "INFO"                  |
| dfs\_domain\_socket\_path                             | ""                      |


[Руководство по конфигурации HDFS](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) поможет обьяснить назначения некоторых параметров.


#### Расширенные параметры для ClickHouse {#clickhouse-extras}

| **параметр**                                          | **по умолчанию**        |
| -                                                     | -                       |
|hadoop\_kerberos\_keytab                               | ""                      |
|hadoop\_kerberos\_principal                            | ""                      |
|hadoop\_kerberos\_kinit\_command                       | kinit                   |

### Ограничения {#limitations}
  * `hadoop_security_kerberos_ticket_cache_path` и `libhdfs3_conf` могут быть определены только на глобальном, а не на пользовательском уровне

## Поддержка Kerberos {#kerberos-support}

Если параметр `hadoop_security_authentication` имеет значение `kerberos`, ClickHouse аутентифицируется с помощью Kerberos.
[Расширенные параметры](#clickhouse-extras) и `hadoop_security_kerberos_ticket_cache_path` помогают сделать это.
Обратите внимание что из-за ограничений libhdfs3 поддерживается только устаревший метод аутентификации,
коммуникация с узлами данных не защищена SASL (`HADOOP_SECURE_DN_USER` надежный показатель такого
подхода к безопасности). Используйте `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` для примера настроек.

Если `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` или `hadoop_kerberos_kinit_command` указаны в настройках, `kinit` будет вызван. `hadoop_kerberos_keytab` и `hadoop_kerberos_principal` обязательны в этом случае. Необходимо также будет установить `kinit` и файлы конфигурации krb5.

## Виртуальные столбцы {#virtual-columns}

-   `_path` — Путь к файлу.
-   `_file` — Имя файла.

**См. также**

-   [Виртуальные колонки](../../../engines/table-engines/index.md#table_engines-virtual_columns)
