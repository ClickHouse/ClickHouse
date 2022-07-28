# system.storage_policies {#system_tables-storage_policies}

Содержит информацию о политиках хранения и томах, заданных в [конфигурации сервера](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Столбцы:

-   `policy_name` ([String](../../sql-reference/data-types/string.md)) — имя политики хранения.
-   `volume_name` ([String](../../sql-reference/data-types/string.md)) — имя тома, который содержится в политике хранения.
-   `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — порядковый номер тома согласно конфигурации.
-   `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — имена дисков, содержащихся в политике хранения.
-   `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — максимальный размер куска данных, который может храниться на дисках тома (0 — без ограничений).
-   `move_factor` — доля доступного свободного места на томе, если места становится меньше, то данные начнут перемещение на следующий том, если он есть (по умолчанию 0.1).
-   `prefer_not_to_merge` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Значение настройки `prefer_not_to_merge`. Если данная настройка включена, то слияние данных, хранящихся на данном томе, не допускается. Это позволяет контролировать работу ClickHouse с медленными дисками.

Если политика хранения содержит несколько томов, то каждому тому соответствует отдельная запись в таблице.

