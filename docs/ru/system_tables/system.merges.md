# system.merges

Содержит информацию о производящихся прямо сейчас слияниях для таблиц семейства MergeTree.

Столбцы:

```text
database String                    - имя базы данных, в которой находится таблица
table String                       - имя таблицы
elapsed Float64                    - время в секундах, прошедшее от начала выполнения слияния
progress Float64                   - доля выполненной работы от 0 до 1
num_parts UInt64                   - количество сливаемых кусков
result_part_name String            - имя куска, который будет образован в результате слияния
total_size_bytes_compressed UInt64 - суммарный размер сжатых данных сливаемых кусков
total_size_marks UInt64            - суммарное количество засечек в сливаемых кусках
bytes_read_uncompressed UInt64     - количество прочитанных байт, разжатых
rows_read UInt64                   - количество прочитанных строк
bytes_written_uncompressed UInt64  - количество записанных байт, несжатых
rows_written UInt64                - количество записанных строк
```
