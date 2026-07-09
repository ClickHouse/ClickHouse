---
description: 'Движок таблицы File хранит данные в файле в одном из поддерживаемых
  файловых форматов (`TabSeparated`, `Native` и т. д.).'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'Движок таблицы File'
doc_type: 'reference'
---

Движок таблицы File хранит данные в файле в одном из поддерживаемых [файловых форматов](/ru/interfaces/formats#formats-overview) (`TabSeparated`, `Native` и т. д.).

Сценарии использования:

* Экспорт данных из ClickHouse в файл.
* Преобразование данных из одного формата в другой.
* Обновление данных в ClickHouse путем редактирования файла на диске.

:::note
Этот движок пока недоступен в ClickHouse Cloud; вместо него [используйте табличную функцию S3](/ru/sql-reference/table-functions/s3.md).
:::

<div id="usage-in-clickhouse-server">
  ## Использование в ClickHouse Server
</div>

```sql
File(Format)
```

Параметр `Format` задаёт один из доступных форматов файлов. Для выполнения
запросов `SELECT` формат должен поддерживаться для ввода, а для выполнения
запросов `INSERT` — для вывода. Список доступных форматов приведён в разделе
[Форматы](/ru/interfaces/formats#formats-overview).

ClickHouse не позволяет указывать путь в файловой системе для `File`. Вместо этого используется каталог, заданный параметром [path](../../../operations/server-configuration-parameters/settings.md) в конфигурации сервера.

При создании таблицы с помощью `File(Format)` в этом каталоге создаётся пустой подкаталог. Когда в эту таблицу записываются данные, они помещаются в файл `data.Format` внутри этого подкаталога.

Вы можете вручную создать этот подкаталог и файл в файловой системе сервера, а затем [ATTACH](../../../sql-reference/statements/attach.md) его к метаданным таблицы с тем же именем, чтобы затем выполнять запросы к данным из этого файла.

:::note
Будьте осторожны с этой возможностью, поскольку ClickHouse не отслеживает внешние изменения таких файлов. Результат одновременной записи через ClickHouse и вне ClickHouse не определён.
:::

<div id="example">
  ## Пример
</div>

**1.** Создайте таблицу `file_engine_table`:

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

По умолчанию ClickHouse создаст каталог `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Вручную создайте файл `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` со следующим содержимым:

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Выполните запрос к данным:

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## Использование в ClickHouse-local
</div>

В [clickhouse-local](../../../operations/utilities/clickhouse-local.md) движок File помимо `Format` принимает путь к файлу. Стандартные потоки ввода/вывода можно указать с помощью числовых или человекочитаемых имен, например `0` или `stdin`, `1` или `stdout`. Также можно читать и записывать сжатые файлы, используя дополнительный параметр движка или расширение файла (`gz`, `br` или `xz`).

**Пример:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## Детали реализации
</div>

* Несколько запросов `SELECT` могут выполняться одновременно, но запросы `INSERT` будут выполняться по очереди.
* Поддерживается создание нового файла с помощью запроса `INSERT`.
* Если файл существует, `INSERT` добавит в него новые значения.
* Не поддерживаются:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * Индексы
  * Репликация

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — необязателен. Данные можно разбивать на отдельные файлы по ключу партиционирования. В большинстве случаев ключ партиционирования не нужен, а если он всё же нужен, то, как правило, достаточно партиционирования по месяцам. Партиционирование не ускоряет запросы (в отличие от выражения ORDER BY). Никогда не используйте слишком мелкое партиционирование. Не партиционируйте данные по идентификаторам или именам клиентов (вместо этого сделайте идентификатор или имя клиента первым столбцом в выражении ORDER BY).

Для партиционирования по месяцам используйте выражение `toYYYYMM(date_column)`, где `date_column` — столбец с датой типа [Date](/ru/sql-reference/data-types/date.md). Имена партиций здесь имеют формат `"YYYYMM"`.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер неизвестен, значение равно `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение равно `NULL`.

<div id="settings">
  ## Настройки
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ru/operations/settings/settings#engine_file_empty_if_not_exists) - позволяет возвращать пустые данные из несуществующего файла. По умолчанию отключено.
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/ru/operations/settings/settings#engine_file_truncate_on_insert) - позволяет очищать файл перед вставкой в него. По умолчанию отключено.
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ru/operations/settings/settings.md#engine_file_allow_create_multiple_files) - позволяет создавать новый файл при каждой вставке, если формат имеет суффикс. По умолчанию отключено.
* [engine&#95;file&#95;skip&#95;empty&#95;files](/ru/operations/settings/settings.md#engine_file_skip_empty_files) - позволяет пропускать пустые файлы при чтении. По умолчанию отключено.
* [storage&#95;file&#95;read&#95;method](/ru/operations/settings/settings#engine_file_empty_if_not_exists) - метод чтения данных из файла хранилища; возможные значения: `read`, `pread`, `mmap`. Метод `mmap` не применяется к clickhouse-server (он предназначен для clickhouse-local). Значение по умолчанию: `pread` для clickhouse-server, `mmap` для clickhouse-local.