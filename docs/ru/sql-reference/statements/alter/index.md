---
toc_priority: 35
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

Изменение структуры таблицы.

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

В запросе указывается список из одного или более действий через запятую.
Каждое действие — операция над столбцом.

Большинство запросов `ALTER` изменяют настройки таблицы или данные:

-   [COLUMN](../../../sql-reference/statements/alter/column.md)
-   [PARTITION](../../../sql-reference/statements/alter/partition.md)
-   [DELETE](../../../sql-reference/statements/alter/delete.md)
-   [UPDATE](../../../sql-reference/statements/alter/update.md)
-   [ORDER BY](../../../sql-reference/statements/alter/order-by.md)
-   [INDEX](../../../sql-reference/statements/alter/index/index.md)
-   [CONSTRAINT](../../../sql-reference/statements/alter/constraint.md)
-   [TTL](../../../sql-reference/statements/alter/ttl.md)

!!! note "Note"
    Запрос `ALTER` поддерживается только для таблиц типа `*MergeTree`, а также `Merge` и `Distributed`. Запрос имеет несколько вариантов.

Следующие запросы `ALTER` изменяют сущности, связанные с управлением доступом на основе ролей:

-   [USER](../../../sql-reference/statements/alter/user.md)
-   [ROLE](../../../sql-reference/statements/alter/role.md)
-   [QUOTA](../../../sql-reference/statements/alter/quota.md)
-   [ROW POLICY](../../../sql-reference/statements/alter/row-policy.md)
-   [SETTINGS PROFILE](../../../sql-reference/statements/alter/settings-profile.md)

### Мутации {#mutations}

Мутации - разновидность запроса ALTER, позволяющая изменять или удалять данные в таблице. В отличие от стандартных запросов [ALTER TABLE … DELETE](../../../sql-reference/statements/alter/delete.md) и [ALTER TABLE … UPDATE](../../../sql-reference/statements/alter/update.md), рассчитанных на точечное изменение данных, область применения мутаций - достаточно тяжёлые изменения, затрагивающие много строк в таблице. Поддержана для движков таблиц семейства [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), в том числе для движков с репликацией.

Конвертировать существующие таблицы для работы с мутациями не нужно. Но после применения первой мутации формат данных таблицы становится несовместимым с предыдущими версиями и откатиться на предыдущую версию уже не получится.

На данный момент доступны команды:

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

Команда перестроит вторичный индекс `name` для партиции `partition_name`.

В одном запросе можно указать несколько команд через запятую.

Для `*MergeTree`-таблиц мутации выполняются, перезаписывая данные по кускам (parts). При этом атомарности нет — куски заменяются на помутированные по мере выполнения и запрос `SELECT`, заданный во время выполнения мутации, увидит данные как из измененных кусков, так и из кусков, которые еще не были изменены.

Мутации линейно упорядочены между собой и накладываются на каждый кусок в порядке добавления. Мутации также упорядочены со вставками - гарантируется, что данные, вставленные в таблицу до начала выполнения запроса мутации, будут изменены, а данные, вставленные после окончания запроса мутации, изменены не будут. При этом мутации никак не блокируют вставки.

Запрос завершается немедленно после добавления информации о мутации (для реплицированных таблиц - в ZooKeeper, для нереплицированных - на файловую систему). Сама мутация выполняется асинхронно, используя настройки системного профиля. Следить за ходом её выполнения можно по таблице [`system.mutations`](../../../operations/system-tables/mutations.md#system_tables-mutations). Добавленные мутации будут выполняться до конца даже в случае перезапуска серверов ClickHouse. Откатить мутацию после её добавления нельзя, но если мутация по какой-то причине не может выполниться до конца, её можно остановить с помощью запроса [`KILL MUTATION`](../../../sql-reference/statements/kill.md#kill-mutation).

Записи о последних выполненных мутациях удаляются не сразу (количество сохраняемых мутаций определяется параметром движка таблиц `finished_mutations_to_keep`). Более старые записи удаляются.

### Синхронность запросов ALTER {#synchronicity-of-alter-queries}

Для нереплицируемых таблиц, все запросы `ALTER` выполняются синхронно. Для реплицируемых таблиц, запрос всего лишь добавляет инструкцию по соответствующим действиям в `ZooKeeper`, а сами действия осуществляются при первой возможности. Но при этом, запрос может ждать завершения выполнения этих действий на всех репликах.

Для запросов `ALTER ... ATTACH|DETACH|DROP` можно настроить ожидание, с помощью настройки `replication_alter_partitions_sync`.
Возможные значения: `0` - не ждать, `1` - ждать выполнения только у себя (по умолчанию), `2` - ждать всех.

Для запросов `ALTER TABLE ... UPDATE|DELETE` синхронность выполнения определяется настройкой [mutations_sync](../../../operations/settings/settings.md#mutations_sync). 

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/index/) <!--hide-->