# system.parts_columns {#system_tables-parts_columns}

Содержит информацию о кусках данных и столбцах таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Каждая строка описывает один кусок данных.

Столбцы:

-   `partition` ([String](../../sql-reference/data-types/string.md)) — имя партиции. Что такое партиция вы можете узнать из описания запроса [ALTER](../../sql-reference/statements/alter/index.md#query_language_queries_alter).

    Форматы:

    -   `YYYYMM` для автоматической схемы партиционирования по месяцам.
    -   `any_string` при партиционировании вручную.

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя куска данных.

-   `part_type` ([String](../../sql-reference/data-types/string.md)) — формат хранения данных.

    Возможные значения:

    -   `Wide` — каждая колонка хранится в отдельном файле. 
    -   `Compact` — все колонки хранятся в одном файле. 

    Формат хранения данных определяется настройками `min_bytes_for_wide_part` и `min_rows_for_wide_part` таблицы [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). 

-   `active` ([UInt8](../../sql-reference/data-types/int-uint.md)) — признак активности. Если кусок данных активен, то он используется таблицей, в противном случае он будет удален. Неактивные куски остаются после слияний.

-   `marks` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество засечек. Чтобы получить примерное количество строк в куске данных, умножьте `marks` на гранулированность индекса (обычно 8192).

-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество строк.

-   `bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер всех файлов кусков данных в байтах.

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер сжатой информации в куске данных. Размер всех дополнительных файлов (например, файлов с засечками) не учитывается.

-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер распакованной информации в куске данных. Размер всех дополнительных файлов (например, файлов с засечками) не учитывается.

-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер файла с засечками.

-   `modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время модификации директории с куском данных. Обычно соответствует времени создания куска.

-   `remove_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время, когда кусок данных стал неактивным.

-   `refcount` ([UInt32](../../sql-reference/data-types/int-uint.md)) — количество мест, в котором кусок данных используется. Значение больше 2 говорит о том, что кусок участвует в запросах или в слияниях.

-   `min_date` ([Date](../../sql-reference/data-types/date.md)) — минимальное значение ключа даты в куске данных.

-   `max_date` ([Date](../../sql-reference/data-types/date.md)) — максимальное значение ключа даты в куске данных.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — ID партиции.

-   `min_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) — минимальное число кусков данных, из которых состоит текущий после слияния.

-   `max_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) — максимальное число кусков данных, из которых состоит текущий после слияния.

-   `level` ([UInt32](../../sql-reference/data-types/int-uint.md)) — глубина дерева слияний. Если слияний не было, то `level=0`.

-   `data_version` ([UInt64](../../sql-reference/data-types/int-uint.md)) — число, которое используется для определения того, какие мутации необходимо применить к куску данных (мутации с версией большей, чем `data_version`).

-   `primary_key_bytes_in_memory` ([UInt64](../../sql-reference/data-types/int-uint.md)) — объём памяти в байтах, занимаемой значениями первичных ключей.

-   `primary_key_bytes_in_memory_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md)) — объём памяти в байтах, выделенный для размещения первичных ключей.

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.

-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.

-   `engine` ([String](../../sql-reference/data-types/string.md)) — имя движка таблицы, без параметров.

-   `disk_name` ([String](../../sql-reference/data-types/string.md)) — имя диска, на котором находится кусок данных.

-   `path` ([String](../../sql-reference/data-types/string.md)) — абсолютный путь к папке с файлами кусков данных.

-   `column` ([String](../../sql-reference/data-types/string.md)) — имя столбца.

-   `type` ([String](../../sql-reference/data-types/string.md)) — тип столбца.

-   `column_position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — порядковый номер столбца (нумерация начинается с 1).

-   `default_kind` ([String](../../sql-reference/data-types/string.md)) — тип выражения (`DEFAULT`, `MATERIALIZED`, `ALIAS`) для значения по умолчанию или пустая строка.

-   `default_expression` ([String](../../sql-reference/data-types/string.md)) — выражение для значения по умолчанию или пустая строка.

-   `column_bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер столбца в байтах.

-   `column_data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер сжатой информации в столбце в байтах.

-   `column_data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — общий размер распакованной информации в столбце в байтах.

-   `column_marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер столбца с засечками в байтах.

-   `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — алиас для `bytes_on_disk`.

-   `marks_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — алиас для `marks_bytes`.

**Пример**

``` sql
SELECT * FROM system.parts_columns LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_2_1
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  2
bytes_on_disk:                         155
data_compressed_bytes:                 56
data_uncompressed_bytes:               4
marks_bytes:                           96
modification_time:                     2020-09-23 10:13:36
remove_time:                           2106-02-07 06:28:15
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
partition_id:                          all
min_block_number:                      1
max_block_number:                      2
level:                                 1
data_version:                          1
primary_key_bytes_in_memory:           2
primary_key_bytes_in_memory_allocated: 64
database:                              default
table:                                 53r93yleapyears
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/53r93yleapyears/all_1_2_1/
column:                                id
type:                                  Int8
column_position:                       1
default_kind:
default_expression:
column_bytes_on_disk:                  76
column_data_compressed_bytes:          28
column_data_uncompressed_bytes:        2
column_marks_bytes:                    48
```

**Смотрите также**

-   [Движок MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)

[Оригинальная статья](https://clickhouse.tech/docs/en/operations/system_tables/parts_columns) <!--hide-->
