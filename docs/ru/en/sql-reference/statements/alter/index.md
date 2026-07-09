---
description: 'Документация по ALTER'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

Большинство запросов `ALTER TABLE` изменяют настройки таблицы или данные:

| Модификатор                                                                 |
| --------------------------------------------------------------------------- |
| [COLUMN](/ru/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/ru/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/ru/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/ru/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/ru/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/ru/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/ru/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/ru/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/ru/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/ru/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/ru/sql-reference/statements/alter/apply-patches.md)           |

:::note
Большинство запросов `ALTER TABLE` поддерживаются только для [таблиц семейства *MergeTree](/ru/engines/table-engines/mergetree-family/index.md), [Merge](/ru/engines/table-engines/special/merge.md) и [Distributed](/ru/engines/table-engines/special/distributed.md).
:::

Эти операторы `ALTER` изменяют представления:

| Оператор                                                                | Описание                                                                       |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| [ALTER TABLE ... MODIFY QUERY](/ru/sql-reference/statements/alter/view.md) | Изменяет структуру [materialized view](/ru/sql-reference/statements/create/view). |

Эти операторы `ALTER` изменяют сущности, связанные с ролевым управлением доступом:

| Оператор                                                                |
| ----------------------------------------------------------------------- |
| [USER](/ru/sql-reference/statements/alter/user.md)                         |
| [ROLE](/ru/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/ru/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/ru/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/ru/sql-reference/statements/alter/settings-profile.md) |

| Оператор                                                                      | Описание                                                                                            |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/ru/sql-reference/statements/alter/comment.md)  | Добавляет, изменяет или удаляет комментарии к таблице независимо от того, были ли они заданы ранее. |
| [ALTER NAMED COLLECTION](/ru/sql-reference/statements/alter/named-collection.md) | Изменяет [именованные коллекции](/ru/operations/named-collections.md).                                 |

<div id="mutations">
  ## Мутации
</div>

Запросы `ALTER`, предназначенные для изменения данных таблицы, реализованы с помощью механизма, называемого «мутациями», в первую очередь [ALTER TABLE ... DELETE](/ru/sql-reference/statements/alter/delete.md) и [ALTER TABLE ... UPDATE](/ru/sql-reference/statements/alter/update.md). Это асинхронные фоновые процессы, похожие на слияния в таблицах [MergeTree](/ru/engines/table-engines/mergetree-family/index.md), которые создают новые «мутированные» версии частей.

Для таблиц `*MergeTree` мутации выполняются путем **перезаписи частей данных целиком**.
Атомарности здесь нет: части заменяются мутированными, как только они готовы, и запрос `SELECT`, начавший выполняться во время мутации, увидит данные как из уже мутированных частей, так и из тех, которые еще не были мутированы.

Мутации полностью упорядочены по порядку создания и применяются к каждой части именно в этом порядке. Мутации также частично упорядочены относительно запросов `INSERT INTO`: данные, вставленные в таблицу до отправки мутации, будут мутированы, а данные, вставленные после, — нет. Обратите внимание, что мутации никак не блокируют вставки.

Запрос мутации возвращает результат сразу после добавления записи о мутации (для реплицируемых таблиц — в ZooKeeper, для нереплицируемых — в файловую систему). Сама мутация выполняется асинхронно с использованием настроек системного профиля. Чтобы отслеживать ход выполнения мутаций, можно использовать таблицу [`system.mutations`](/ru/operations/system-tables/mutations). Успешно отправленная мутация продолжит выполняться даже после перезапуска серверов ClickHouse. После отправки мутацию нельзя откатить, но если она по какой-то причине зависла, ее можно отменить с помощью запроса [`KILL MUTATION`](/ru/sql-reference/statements/kill.md/#kill-mutation).

Записи о завершенных мутациях удаляются не сразу (количество сохраняемых записей определяется параметром движка хранилища `finished_mutations_to_keep`). Более старые записи о мутациях удаляются.

<div id="synchronicity-of-alter-queries">
  ## Синхронность запросов `ALTER`
</div>

Для нереплицируемых таблиц все запросы `ALTER` выполняются синхронно. Для реплицируемых таблиц запрос лишь добавляет в `ZooKeeper` инструкции для соответствующих действий, а сами действия выполняются при первой возможности. Однако запрос может ждать, пока эти действия не будут завершены на всех репликах.

Для запросов `ALTER`, создающих мутации (например, включая, помимо прочего, `UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `APPLY PATCHES`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC`), синхронность определяется настройкой [mutations&#95;sync](/ru/operations/settings/settings.md/#mutations_sync).

Для других запросов `ALTER`, которые изменяют только метаданные, можно использовать настройку [alter&#95;sync](/ru/operations/settings/settings#alter_sync), чтобы задать ожидание.

С помощью настройки [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ru/operations/settings/settings#replication_wait_for_inactive_replica_timeout) можно указать, как долго (в секундах) ждать, пока неактивные реплики выполнят все запросы `ALTER`.

:::note
Для всех запросов `ALTER`, если `alter_sync = 2` и некоторые реплики остаются неактивными дольше времени, указанного в настройке `replication_wait_for_inactive_replica_timeout`, генерируется исключение `UNFINISHED`.
:::

<div id="related-content">
  ## Материалы по теме
</div>

* Блог: [Обработка обновлений и удалений в ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)