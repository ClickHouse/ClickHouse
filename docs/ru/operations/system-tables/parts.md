# system.parts {#system_tables-parts}

Содержит информацию о кусках данных таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

Каждая строка описывает один кусок данных.

Столбцы:

-   `partition` ([String](../../sql-reference/data-types/string.md)) – имя партиции. Что такое партиция можно узнать из описания запроса [ALTER](../../sql-reference/statements/alter/index.md#query_language_queries_alter).

    Форматы:

    -   `YYYYMM` для автоматической схемы партиционирования по месяцам.
    -   `any_string` при партиционировании вручную.

-   `name` ([String](../../sql-reference/data-types/string.md)) – имя куска.

-   `part_type` ([String](../../sql-reference/data-types/string.md)) — формат хранения данных.

    Возможные значения:

    -   `Wide` — каждая колонка хранится в отдельном файле.
    -   `Compact` — все колонки хранятся в одном файле.

    Формат хранения данных определяется настройками `min_bytes_for_wide_part` и `min_rows_for_wide_part` таблицы [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

-   `active` ([UInt8](../../sql-reference/data-types/int-uint.md)) – признак активности. Если кусок активен, то он используется таблицей, в противном случает он будет удален. Неактивные куски остаются после слияний.

-   `marks` ([UInt64](../../sql-reference/data-types/int-uint.md)) – количество засечек. Чтобы получить примерное количество строк в куске, умножьте `marks` на гранулированность индекса (обычно 8192).

-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) – количество строк.

-   `bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) – общий размер всех файлов кусков данных в байтах.

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – общий размер сжатой информации в куске данных. Размер всех дополнительных файлов (например, файлов с засечками) не учитывается.

-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – общий размер распакованной информации куска данных. Размер всех дополнительных файлов (например, файлов с засечками) не учитывается.

-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – размер файла с засечками.

-   `secondary_indices_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – общий размер сжатых данных для вторичных индексов в куске данных. Вспомогательные файлы (например, файлы с засечками) не включены.

-   `secondary_indices_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – общий размер несжатых данных для вторичных индексов в куске данных. Вспомогательные файлы (например, файлы с засечками) не включены.

- `secondary_indices_marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – размер файла с засечками для вторичных индексов.

-   `modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – время модификации директории с куском данных. Обычно соответствует времени создания куска.

-   `remove_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – время, когда кусок стал неактивным.

-   `refcount` ([UInt32](../../sql-reference/data-types/int-uint.md)) – количество мест, в котором кусок используется. Значение больше 2 говорит о том, что кусок участвует в запросах или в слияниях.

-   `min_date` ([Date](../../sql-reference/data-types/date.md)) – минимальное значение ключа даты в куске данных.

-   `max_date` ([Date](../../sql-reference/data-types/date.md)) – максимальное значение ключа даты в куске данных.

-   `min_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – минимальное значение даты и времени в куске данных.

-   `max_time`([DateTime](../../sql-reference/data-types/datetime.md)) – максимальное значение даты и времени в куске данных.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) – ID партиции.

-   `min_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) – минимальное число кусков, из которых состоит текущий после слияния.

-   `max_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) – максимальное число кусков, из которых состоит текущий после слияния.

-   `level` ([UInt32](../../sql-reference/data-types/int-uint.md)) - глубина дерева слияний. Если слияний не было, то `level=0`.

-   `data_version` ([UInt64](../../sql-reference/data-types/int-uint.md)) – число, которое используется для определения того, какие мутации необходимо применить к куску данных (мутации с версией большей, чем `data_version`).

-   `primary_key_bytes_in_memory` ([UInt64](../../sql-reference/data-types/int-uint.md)) – объём памяти (в байтах), занимаемой значениями первичных ключей.

-   `primary_key_bytes_in_memory_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md)) – объём памяти (в байтах) выделенный для размещения первичных ключей.

-   `is_frozen` ([UInt8](../../sql-reference/data-types/int-uint.md)) – Признак, показывающий существование бэкапа партиции. 1, бэкап есть. 0, бэкапа нет. Смотрите раздел [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md#alter_freeze-partition).

-   `database` ([String](../../sql-reference/data-types/string.md)) – имя базы данных.

-   `table` ([String](../../sql-reference/data-types/string.md)) – имя таблицы.

-   `engine` ([String](../../sql-reference/data-types/string.md)) – имя движка таблицы, без параметров.

-   `path` ([String](../../sql-reference/data-types/string.md)) – абсолютный путь к папке с файлами кусков данных.

-   `disk` ([String](../../sql-reference/data-types/string.md)) – имя диска, на котором находится кусок данных.

-   `hash_of_all_files` ([String](../../sql-reference/data-types/string.md)) – значение [sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128) для сжатых файлов.

-   `hash_of_uncompressed_files` ([String](../../sql-reference/data-types/string.md)) – значение [sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128) несжатых файлов (файлы с засечками, первичным ключом и пр.)

-   `uncompressed_hash_of_compressed_files` ([String](../../sql-reference/data-types/string.md)) – значение [sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128) данных в сжатых файлах как если бы они были разжатыми.

-   `delete_ttl_info_min` ([DateTime](../../sql-reference/data-types/datetime.md)) — Минимальное значение ключа даты и времени для правила [TTL DELETE](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

-   `delete_ttl_info_max` ([DateTime](../../sql-reference/data-types/datetime.md)) — Максимальное значение ключа даты и времени для правила [TTL DELETE](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

-   `move_ttl_info.expression` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Массив выражений. Каждое выражение задаёт правило [TTL MOVE](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

        :::note "Предупреждение"
        Массив выражений `move_ttl_info.expression` используется, в основном, для обратной совместимости. Для работы с правилами `TTL MOVE` лучше использовать поля `move_ttl_info.min` и `move_ttl_info.max`.
        :::
-   `move_ttl_info.min` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — Массив значений. Каждый элемент массива задаёт минимальное значение ключа даты и времени для правила [TTL MOVE](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

-   `move_ttl_info.max` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — Массив значений. Каждый элемент массива задаёт максимальное значение ключа даты и времени для правила [TTL MOVE](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

-   `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – алиас для `bytes_on_disk`.

-   `marks_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) – алиас для `marks_bytes`.

**Пример**

``` sql
SELECT * FROM system.parts LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_4_1_6
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  6
bytes_on_disk:                         310
data_compressed_bytes:                 157
data_uncompressed_bytes:               91
secondary_indices_compressed_bytes:    58
secondary_indices_uncompressed_bytes:  6
secondary_indices_marks_bytes:         48
marks_bytes:                           144
modification_time:                     2020-06-18 13:01:49
remove_time:                           0000-00-00 00:00:00
refcount:                              1
min_date:                              0000-00-00
max_date:                              0000-00-00
min_time:                              0000-00-00 00:00:00
max_time:                              0000-00-00 00:00:00
partition_id:                          all
min_block_number:                      1
max_block_number:                      4
level:                                 1
data_version:                          6
primary_key_bytes_in_memory:           8
primary_key_bytes_in_memory_allocated: 64
is_frozen:                             0
database:                              default
table:                                 months
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/months/all_1_4_1_6/
hash_of_all_files:                     2d0657a16d9430824d35e327fcbd87bf
hash_of_uncompressed_files:            84950cc30ba867c77a408ae21332ba29
uncompressed_hash_of_compressed_files: 1ad78f1c6843bbfb99a2c931abe7df7d
delete_ttl_info_min:                   0000-00-00 00:00:00
delete_ttl_info_max:                   0000-00-00 00:00:00
move_ttl_info.expression:              []
move_ttl_info.min:                     []
move_ttl_info.max:                     []
```

**См. также**

-   [Движок MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)
-   [TTL для столбцов и таблиц](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)

