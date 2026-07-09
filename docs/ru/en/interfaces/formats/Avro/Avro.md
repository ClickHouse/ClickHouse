---
alias: []
description: 'Документация для формата Avro'
input_format: true
keywords: ['Avro']
output_format: true
slug: /interfaces/formats/Avro
title: 'Avro'
doc_type: 'reference'
---

import DataTypeMapping from './_snippets/data-types-matching.md'

| Вход | Выход | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

[Apache Avro](https://avro.apache.org/) — это строко-ориентированный формат сериализации, использующий двоичное кодирование для эффективной обработки данных. Формат `Avro` поддерживает чтение и запись [файлов данных Avro](https://avro.apache.org/docs/current/specification/#object-container-files). Этот формат предполагает самоописывающиеся сообщения со встроенной схемой. Если вы используете Avro с реестром схем, обратитесь к формату [`AvroConfluent`](./AvroConfluent.md).

<div id="data-type-mapping">
  ## Сопоставление типов данных
</div>

<DataTypeMapping />

<div id="format-settings">
  ## Настройки формата
</div>

| Setting                                    | Description                                                                                                                                                                                | Default |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `input_format_avro_allow_missing_fields`   | Использовать ли значение по умолчанию вместо генерации ошибки, если поле не найдено в схеме.                                                                                               | `0`     |
| `input_format_avro_null_as_default`        | Использовать ли значение по умолчанию вместо генерации ошибки при вставке значения `null` в столбец, не допускающий `null`.                                                                | `0`     |
| `output_format_avro_codec`                 | Алгоритм сжатия для выходных файлов Avro. Possible values: `null`, `deflate`, `snappy`, `zstd`.                                                                                            |         |
| `output_format_avro_sync_interval`         | Частота маркеров синхронизации в файлах Avro (в байтах).                                                                                                                                   | `16384` |
| `output_format_avro_string_column_pattern` | Шаблон регулярного выражения для определения столбцов `String` при сопоставлении со строковым типом Avro. По умолчанию столбцы ClickHouse типа `String` записываются как тип Avro `bytes`. |         |
| `output_format_avro_rows_in_file`          | Максимальное количество строк в одном выходном файле Avro. При достижении этого предела создаётся новый файл (если система хранилища поддерживает разделение файлов).                      | `1`     |

<div id="examples">
  ## Примеры
</div>

<div id="reading-avro-data">
  ### Чтение данных Avro
</div>

Чтобы прочитать данные из файла Avro в таблицу ClickHouse:

```bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

Корневая схема импортируемого Avro-файла должна иметь тип `record`.

Чтобы определить соответствие между столбцами таблицы и полями схемы Avro, ClickHouse сравнивает их имена.
Это сравнение чувствительно к регистру, а неиспользуемые поля пропускаются.

Типы данных столбцов таблицы ClickHouse могут отличаться от типов соответствующих полей вставляемых Avro-данных. При вставке данных ClickHouse интерпретирует типы данных в соответствии с таблицей выше, а затем [преобразует](/ru/sql-reference/functions/type-conversion-functions#CAST) данные к соответствующему типу столбца.

При импорте данных, если поле не найдено в схеме и включена настройка [`input_format_avro_allow_missing_fields`](/ru/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields), вместо генерации ошибки будет использовано значение по умолчанию.

<div id="writing-avro-data">
  ### Запись данных в формате Avro
</div>

Чтобы записать данные из таблицы ClickHouse в файл Avro:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Имена столбцов должны:

* Начинаться с `[A-Za-z_]`
* Далее содержать только `[A-Za-z0-9_]`

Сжатие выходных данных и интервал синхронизации для файлов Avro можно настроить с помощью параметров [`output_format_avro_codec`](/ru/operations/settings/settings-formats.md/#output_format_avro_codec) и [`output_format_avro_sync_interval`](/ru/operations/settings/settings-formats.md/#output_format_avro_sync_interval) соответственно.

<div id="inferring-the-avro-schema">
  ### Определение схемы Avro
</div>

С помощью функции ClickHouse [`DESCRIBE`](/ru/sql-reference/statements/describe-table) можно быстро просмотреть автоматически определённую схему файла Avro, как показано в следующем примере.
В этом примере используется URL общедоступного файла Avro из публичного бакета S3 ClickHouse:

```sql
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro', 'Avro');

┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```