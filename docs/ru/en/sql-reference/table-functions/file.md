---
description: 'Движок таблицы, предоставляющий табличный интерфейс для выполнения SELECT из файлов
  и INSERT в файлы, аналогично табличной функции `s3`. Используйте `file` при работе
  с локальными файлами, а `s3` — при работе с бакетами в объектном хранилище, таком
  как S3, GCS или MinIO.'
sidebar_label: 'file'
sidebar_position: 60
slug: /sql-reference/table-functions/file
title: 'file'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="file-table-function">
  # Табличная функция file
</div>

Движок таблицы, предоставляющий табличный интерфейс для `SELECT` из файлов и `INSERT` в файлы, аналогично табличной функции [s3](/ru/sql-reference/table-functions/s3.md). Используйте `file` при работе с локальными файлами, а `s3` — при работе с бакетами в Объектном хранилище, таком как S3, GCS или MinIO.

Функцию `file` можно использовать в запросах `SELECT` и `INSERT` для чтения из файлов и записи в них.

<div id="syntax">
  ## Синтаксис
</div>

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

В запросах `SELECT` `path` также может быть выражением, возвращающим `Array(String)`:

```sql
file(['file1.csv', 'file2.csv'], 'CSV', 'column1 UInt32, column2 UInt32')
```

<div id="arguments">
  ## Аргументы
</div>

| Параметр          | Описание                                                                                                                                                                                                                                                                                                                                                       |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`            | Относительный путь к файлу из [user&#95;files&#95;path](/ru/operations/server-configuration-parameters/settings.md#user_files_path) или `Array(String)` путей в `SELECT`-запросах. В режиме только для чтения поддерживаются следующие [глоб-шаблоны](#globs-in-path): `*`, `?`, `{abc,def}` (где `'abc'` и `'def'` — строки) и `{N..M}` (где `N` и `M` — числа). |
| `path_to_archive` | Относительный путь к архиву zip/tar/7z. Поддерживает те же глоб-шаблоны, что и `path`.                                                                                                                                                                                                                                                                         |
| `format`          | [Формат](/ru/interfaces/formats) файла.                                                                                                                                                                                                                                                                                                                           |
| `structure`       | Структура таблицы. Формат: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                      |
| `compression`     | Существующий тип сжатия при использовании в `SELECT`-запросе или требуемый тип сжатия при использовании в `INSERT`-запросе. Поддерживаются следующие типы сжатия: `gz`, `br`, `xz`, `zst`, `lz4` и `bz2`.                                                                                                                                                      |

:::tip
Если аргумент `structure` не указан, ClickHouse определяет схему на основе самого формата.
Для разных форматов используются разные имена столбцов и типы по умолчанию.
Чтобы увидеть схему для конкретного формата, используйте [`DESC`](/ru/sql-reference/statements/describe-table) с табличной функцией [`format`](/ru/sql-reference/table-functions/format).

Например:

```sql
DESC format(LineAsString, 'Hello\nWorld')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица для чтения данных из файла или записи данных в файл.

<div id="examples-for-writing-to-a-file">
  ## Примеры записи в файл
</div>

<div id="write-to-a-tsv-file">
  ### Запись в TSV-файл
</div>

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

В результате данные записываются в файл `test.tsv`:

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1    2    3
3    2    1
1    3    2
```

<div id="partitioned-write-to-multiple-tsv-files">
  ### Запись в несколько файлов в формате TSV с разбиением по партициям
</div>

Если при вставке данных в табличную функцию типа `file` указано выражение `PARTITION BY`, для каждой партиции создаётся отдельный файл. Разделение данных на отдельные файлы помогает повысить производительность операций чтения.

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

В результате данные записываются в три файла: `test_1.tsv`, `test_2.tsv` и `test_3.tsv`.

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3    2    1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1    3    2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1    2    3
```

<div id="examples-for-reading-from-a-file">
  ## Примеры чтения из файла
</div>

<div id="select-from-a-csv-file">
  ### SELECT из CSV-файла
</div>

Сначала задайте `user_files_path` в конфигурации сервера и подготовьте `test.csv`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Затем загрузите данные из `test.csv` в таблицу и выберите первые две строки:

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="inserting-data-from-a-file-into-a-table">
  ### Вставка данных из файла в таблицу
</div>

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

Чтение данных из `table.csv`, находящегося в `archive1.zip` и/или `archive2.zip`:

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

<div id="globs-in-path">
  ## Глоб-шаблоны в пути
</div>

В путях можно использовать глоб-шаблоны. Файлы должны соответствовать всему шаблону пути, а не только его суффиксу или префиксу. Есть одно исключение: если путь указывает на существующий
каталог и не содержит глоб-шаблонов, к пути неявно добавляется `*`, чтобы
выбрать все файлы в каталоге.

* `*` — Обозначает произвольное количество символов, кроме `/`, включая пустую строку.
* `?` — Обозначает один произвольный символ.
* `{some_string,another_string,yet_another_one}` — Подставляет любую из строк `'some_string', 'another_string', 'yet_another_one'`. Строки могут содержать символ `/`.
* `{N..M}` — Обозначает любое число `>= N` и `<= M`.
* `**` - Обозначает все файлы в каталоге и его подкаталогах рекурсивно.

Конструкции с `{}` аналогичны конструкциям в табличных функциях [remote](remote.md) и [hdfs](hdfs.md).

<div id="examples">
  ## Примеры
</div>

**Пример**

Предположим, имеются следующие файлы со следующими относительными путями:

* `some_dir/some_file_1`
* `some_dir/some_file_2`
* `some_dir/some_file_3`
* `another_dir/some_file_1`
* `another_dir/some_file_2`
* `another_dir/some_file_3`

Выполните запрос, чтобы получить общее количество строк во всех файлах:

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

Альтернативное выражение пути, дающее тот же результат:

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

Запросите общее количество строк в `some_dir`, используя неявный `*`:

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
Если в вашем списке файлов есть числовые диапазоны с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры отдельно или символ `?`.
:::

**Пример**

Выполните запрос, чтобы получить общее количество строк в файлах с именами `file000`, `file001`, ... , `file999`:

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**Пример**

Получите общее количество строк во всех файлах каталога `big_dir/` рекурсивно:

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**Пример**

Рекурсивно выполните запрос, чтобы получить общее количество строк во всех файлах `file002` в любой папке каталога `big_dir/`:

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время изменения неизвестно, значение — `NULL`.

<div id="hive-style-partitioning">
  ## Настройка use_hive_partitioning
</div>

Если для настройки `use_hive_partitioning` установлено значение 1, ClickHouse распознаёт секционирование в стиле Hive в пути (`/name=value/`) и позволяет использовать столбцы партиций как виртуальные столбцы в запросе. Эти виртуальные столбцы будут иметь те же имена, что и в пути с партициями.

**Пример**

Использование виртуального столбца, созданного при секционировании в стиле Hive

```sql
SELECT * FROM file('data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="settings">
  ## Настройки
</div>

| Настройка                                                                                                                               | Описание                                                                                                                                                                       |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/ru/operations/settings/settings#engine_file_empty_if_not_exists)                    | позволяет возвращать пустой результат при чтении из несуществующего файла. По умолчанию отключено.                                                                             |
| [engine&#95;file&#95;truncate&#95;on&#95;insert](/ru/operations/settings/settings#engine_file_truncate_on_insert)                          | позволяет очищать файл перед вставкой данных в него. По умолчанию отключено.                                                                                                   |
| [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/ru/operations/settings/settings.md#engine_file_allow_create_multiple_files) | позволяет создавать новый файл при каждой вставке, если у формата есть суффикс. По умолчанию отключено.                                                                        |
| [engine&#95;file&#95;skip&#95;empty&#95;files](/ru/operations/settings/settings.md#engine_file_skip_empty_files)                           | позволяет пропускать пустые файлы при чтении. По умолчанию отключено.                                                                                                          |
| [storage&#95;file&#95;read&#95;method](/ru/operations/settings/settings#engine_file_empty_if_not_exists)                                   | метод чтения данных из файла хранилища: read, pread или mmap (только для clickhouse-local). Значение по умолчанию: `pread` для clickhouse-server, `mmap` для clickhouse-local. |

<div id="related">
  ## См. также
</div>

* [Виртуальные столбцы](/ru/engines/table-engines/index.md#table_engines-virtual_columns)
* [Переименование файлов после обработки](/ru/operations/settings/settings.md#rename_files_after_processing)