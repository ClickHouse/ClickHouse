---
description: 'Этот движок интегрируется с экосистемой Apache Hadoop и позволяет управлять данными в HDFS через ClickHouse. Он похож на движки File и URL, но предоставляет специфические для Hadoop возможности.'
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'Движок таблицы HDFS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # Движок таблицы HDFS
</div>

<CloudNotSupportedBadge />

Этот движок обеспечивает интеграцию с экосистемой [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop), позволяя управлять данными в [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) через ClickHouse. Он похож на движки [File](/ru/engines/table-engines/special/file) и [URL](/ru/engines/table-engines/special/url), но предоставляет возможности, специфичные для Hadoop.

Эта возможность не поддерживается инженерами ClickHouse, и её качество, как известно, оставляет желать лучшего. Если у вас возникнут проблемы, исправляйте их самостоятельно и отправляйте pull request.

<div id="usage">
  ## Использование
</div>

```sql
ENGINE = HDFS(URI, format)
```

**Параметры движка**

* `URI` - полный URI файла в HDFS. Часть пути в `URI` может содержать глоб-шаблоны. В этом случае таблица будет доступна только для чтения.
* `format` - задаёт один из доступных форматов файлов. Для выполнения
  запросов `SELECT` формат должен поддерживать чтение входных данных, а для выполнения
  запросов `INSERT` — запись выходных данных. Доступные форматы перечислены в разделе
  [Форматы](/ru/sql-reference/formats#formats-overview).
* [PARTITION BY выражение]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — необязателен. В большинстве случаев ключ партиционирования не нужен, а если и нужен, то обычно достаточно партиционирования по месяцам. Партиционирование не ускоряет запросы (в отличие от выражения ORDER BY). Никогда не используйте слишком мелкое партиционирование. Не разбивайте данные на партиции по идентификаторам или именам клиентов (вместо этого сделайте идентификатор или имя клиента первым столбцом в выражении ORDER BY).

Для партиционирования по месяцам используйте выражение `toYYYYMM(date_column)`, где `date_column` — столбец с датой типа [Date](/ru/sql-reference/data-types/date.md). Имена партиций в этом случае имеют формат `"YYYYMM"`.

**Пример:**

**1.** Настройте таблицу `hdfs_engine_table`:

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Заполните файл:

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Запросите данные:

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## Подробности реализации
</div>

* Чтение и запись могут выполняться параллельно.
* Не поддерживаются:

  * операции `ALTER` и `SELECT...SAMPLE`;
  * индексы;
  * [репликация с нулевым копированием](../../../operations/storing-data.md#zero-copy) возможна, но не рекомендуется.

  :::note Репликация с нулевым копированием не готова для продакшна
  В ClickHouse версии 22.8 и выше репликация с нулевым копированием по умолчанию отключена. Эта возможность не рекомендуется для использования в продакшне.
  :::

**Глоб-шаблоны в пути**

Несколько компонентов пути могут содержать глоб-шаблоны. Чтобы файл был обработан, он должен существовать и соответствовать всему шаблону пути. Список файлов определяется во время выполнения `SELECT` (а не в момент `CREATE`).

* `*` — Подставляет любое количество любых символов, кроме `/`, включая пустую строку.
* `?` — Подставляет любой одиночный символ.
* `{some_string,another_string,yet_another_one}` — Подставляет любую из строк `'some_string', 'another_string', 'yet_another_one'`.
* `{N..M}` — Подставляет любое число в диапазоне от N до M включительно.

Конструкции с `{}` аналогичны конструкции в табличной функции [remote](../../../sql-reference/table-functions/remote.md).

**Пример**

1. Предположим, у нас есть несколько файлов в формате TSV со следующими URI в HDFS:

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Есть несколько способов создать таблицу из всех шести файлов:

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Ещё один способ:

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Таблица включает все файлы из обоих каталогов (все файлы должны соответствовать формату и схеме, описанным в запросе):

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
Если в списке файлов есть числовые диапазоны с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры отдельно или символ `?`.
:::

**Пример**

Создайте таблицу с файлами с именами `file000`, `file001`, ... , `file999`:

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## Конфигурация
</div>

Как и GraphiteMergeTree, движок HDFS поддерживает расширенную настройку через файл конфигурации ClickHouse. Можно использовать два ключа конфигурации: глобальный (`hdfs`) и пользовательский (`hdfs_*`). Сначала применяется глобальная конфигурация, а затем — пользовательская (если она задана).

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### Параметры конфигурации
</div>

<div id="supported-by-libhdfs3">
  #### Поддерживается libhdfs3
</div>

| **параметр**                                                            | **значение по умолчанию**         |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

Описание некоторых параметров см. в [справочнике по конфигурации HDFS](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html).

<div id="clickhouse-extras">
  #### Дополнительные параметры ClickHouse
</div>

| **параметр**                      | **значение по умолчанию** |
| --------------------------------- | ------------------------- |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot;              |
| hadoop&#95;kerberos&#95;principal | &quot;&quot;              |
| libhdfs3&#95;conf                 | &quot;&quot;              |

<div id="limitations">
  ### Ограничения
</div>

* `hadoop_security_kerberos_ticket_cache_path` и `libhdfs3_conf` могут быть только глобальными, а не пользовательскими

<div id="kerberos-support">
  ## Поддержка Kerberos
</div>

Если параметр `hadoop_security_authentication` имеет значение `kerberos`, ClickHouse выполняет аутентификацию через Kerberos.
Параметры приведены [здесь](#clickhouse-extras); также может пригодиться `hadoop_security_kerberos_ticket_cache_path`.
Обратите внимание, что из-за ограничений libhdfs3 поддерживается только старый подход:
взаимодействие с datanode не защищено с помощью SASL (`HADOOP_SECURE_DN_USER` — надёжный индикатор такого
подхода к безопасности). В качестве ориентира используйте `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh`.

Если указаны `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` или `hadoop_security_kerberos_ticket_cache_path`, будет использоваться аутентификация Kerberos. В этом случае `hadoop_kerberos_keytab` и `hadoop_kerberos_principal` обязательны.

<div id="namenode-ha">
  ## Поддержка HA для namenode HDFS
</div>

libhdfs3 поддерживает HA для namenode HDFS.

* Скопируйте `hdfs-site.xml` с узла HDFS в `/etc/clickhouse-server/`.
* Добавьте следующий фрагмент в файл конфигурации ClickHouse:

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* Затем используйте значение тега `dfs.nameservices` из `hdfs-site.xml` в качестве адреса `namenode` в URI HDFS. Например, замените `hdfs://appadmin@192.168.101.11:8020/abc/` на `hdfs://appadmin@my_nameservice/abc/`.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.

<div id="storage-settings">
  ## Настройки хранилища
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/ru/operations/settings/settings.md#hdfs_truncate_on_insert) - позволяет обрезать файл перед вставкой в него. По умолчанию отключено.
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/ru/operations/settings/settings.md#hdfs_create_new_file_on_insert) - позволяет создавать новый файл при каждой вставке, если у формата есть суффикс. По умолчанию отключено.
* [hdfs&#95;skip&#95;empty&#95;files](/ru/operations/settings/settings.md#hdfs_skip_empty_files) - позволяет пропускать пустые файлы при чтении. По умолчанию отключено.

**См. также**

* [Виртуальные столбцы](../../../engines/table-engines/index.md#table_engines-virtual_columns)