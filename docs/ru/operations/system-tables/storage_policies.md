# system.storage_policies {#system_tables-storage_policies}

Содержит информацию о политиках хранения и томах, заданных в [конфигурации сервера](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Столбцы:

-   `policy_name` ([String](../../sql-reference/data-types/string.md)) — имя политики хранения.
-   `volume_name` ([String](../../sql-reference/data-types/string.md)) — имя тома, который содержится в политике хранения.
-   `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — порядковый номер тома согласно конфигурации.
-   `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — имена дисков, содержащихся в политике хранения.
-   `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — максимальный размер куска данных, который может храниться на дисках тома (0 — без ограничений).
-   `move_factor` ([Float64](../../sql-reference/data-types/float.md))\` — доля свободного места, при превышении которой данные начинают перемещаться на следующий том.

Если политика хранения содержит несколько томов, то каждому тому соответствует отдельная запись в таблице.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/storage_policies) <!--hide-->
