# system.distribution_queue {#system_tables-distribution_queue}

Содержит информацию о локальных файлах, которые находятся в очереди для отправки на шарды. Эти локальные файлы содержат новые куски, которые создаются путем вставки новых данных в Distributed таблицу в асинхронном режиме.

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.

-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.

-   `data_path` ([String](../../sql-reference/data-types/string.md)) — путь к папке с локальными файлами.

-   `is_blocked` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, указывающий на блокировку отправки локальных файлов на шарды.

-   `error_count` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество ошибок.

-   `data_files` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество локальных файлов в папке.

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер сжатых данных в локальных файлах в байтах.

-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — текстовое сообщение о последней возникшей ошибке, если таковые имеются.

**Пример**

``` sql
SELECT * FROM system.distribution_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:              default
table:                 dist
data_path:             ./store/268/268bc070-3aad-4b1a-9cf2-4987580161af/default@127%2E0%2E0%2E2:9000/
is_blocked:            1
error_count:           0
data_files:            1
data_compressed_bytes: 499
last_exception:        
```

**Смотрите также**

-   [Движок таблиц Distributed](../../engines/table-engines/special/distributed.md)

