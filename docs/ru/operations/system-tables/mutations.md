# system.mutations {#system_tables-mutations}

Таблица содержит информацию о ходе выполнения [мутаций](../../sql-reference/statements/alter/index.md#mutations) таблиц семейства MergeTree. Каждой команде мутации соответствует одна строка таблицы.

Столбцы:

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя БД, к которой была применена мутация.

-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы, к которой была применена мутация.

-   `mutation_id` ([String](../../sql-reference/data-types/string.md)) — ID запроса. Для реплицированных таблиц эти ID соответствуют именам записей в директории `<table_path_in_zookeeper>/mutations/` в ZooKeeper, для нереплицированных — именам файлов в директории с данными таблицы.

-   `command` ([String](../../sql-reference/data-types/string.md)) — команда мутации (часть запроса после `ALTER TABLE [db.]table`).

-   `create_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — дата и время создания мутации.

-   `block_numbers.partition_id` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Для мутаций реплицированных таблиц массив содержит содержит номера партиций (по одной записи для каждой партиции). Для мутаций нереплицированных таблиц массив пустой.

-   `block_numbers.number` ([Array](../../sql-reference/data-types/array.md)([Int64](../../sql-reference/data-types/int-uint.md))) — Для мутаций реплицированных таблиц массив содержит по одной записи для каждой партиции, с номером блока, полученным этой мутацией. В каждой партиции будут изменены только куски, содержащие блоки с номерами меньше чем данный номер.

    Для нереплицированных таблиц нумерация блоков сквозная по партициям. Поэтому массив содержит единственную запись с номером блока, полученным мутацией.

-   `parts_to_do_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — массив с именами кусков данных, которые должны быть изменены для завершения мутации.

-   `parts_to_do` ([Int64](../../sql-reference/data-types/int-uint.md)) — количество кусков данных, которые должны быть изменены для завершения мутации.

-   `is_done` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Признак, завершена ли мутация. Возможные значения:
    -   `1` — мутация завершена,
    -   `0` — мутация еще продолжается.

:::info "Замечание"
    Даже если `parts_to_do = 0`, для реплицированной таблицы возможна ситуация, когда мутация ещё не завершена из-за долго выполняющейся операции `INSERT`, которая добавляет данные, которые нужно будет мутировать.

Если во время мутации какого-либо куска возникли проблемы, заполняются следующие столбцы:

-   `latest_failed_part` ([String](../../sql-reference/data-types/string.md)) — имя последнего куска, мутация которого не удалась.

-   `latest_fail_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — дата и время последней ошибки мутации.

-   `latest_fail_reason` ([String](../../sql-reference/data-types/string.md)) — причина последней ошибки мутации.

**См. также**

-   [Мутации](../../sql-reference/statements/alter/index.md#mutations)
-   [Движок MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)
-   [Репликация данных](../../engines/table-engines/mergetree-family/replication.md) (семейство ReplicatedMergeTree)

