# system.merges {#system-merges}

Содержит информацию о производящихся прямо сейчас слияниях и мутациях кусков для таблиц семейства MergeTree.

Столбцы:

-   `database String` — Имя базы данных, в которой находится таблица.
-   `table String` — Имя таблицы.
-   `elapsed Float64` — Время в секундах, прошедшее от начала выполнения слияния.
-   `progress Float64` — Доля выполненной работы от 0 до 1.
-   `num_parts UInt64` — Количество сливаемых кусков.
-   `result_part_name String` — Имя куска, который будет образован в результате слияния.
-   `is_mutation UInt8` - Является ли данный процесс мутацией куска.
-   `total_size_bytes_compressed UInt64` — Суммарный размер сжатых данных сливаемых кусков.
-   `total_size_marks UInt64` — Суммарное количество засечек в сливаемых кусках.
-   `bytes_read_uncompressed UInt64` — Количество прочитанных байт, разжатых.
-   `rows_read UInt64` — Количество прочитанных строк.
-   `bytes_written_uncompressed UInt64` — Количество записанных байт, несжатых.
-   `rows_written UInt64` — Количество записанных строк.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/merges) <!--hide-->
