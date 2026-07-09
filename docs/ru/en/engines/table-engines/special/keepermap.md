---
description: 'Этот движок позволяет использовать кластер Keeper/ZooKeeper как
  согласованное хранилище типа ключ-значение с линеаризуемой записью и последовательно согласованным чтением.'
sidebar_label: 'KeeperMap'
sidebar_position: 150
slug: /engines/table-engines/special/keeper-map
title: 'Движок таблицы KeeperMap'
doc_type: 'reference'
---

Этот движок позволяет использовать кластер Keeper/ZooKeeper как согласованное хранилище типа ключ-значение с линеаризуемой записью и последовательно согласованным чтением.

Чтобы включить движок таблицы KeeperMap, необходимо задать путь в ZooKeeper, где будут храниться таблицы, с помощью параметра конфигурации `<keeper_map_path_prefix>`.

Например:

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

где path может быть любым другим допустимым путём в ZooKeeper.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = KeeperMap(root_path, [keys_limit]) PRIMARY KEY(primary_key_name)
```

Параметры движка:

* `root_path` — путь в ZooKeeper, где будет храниться `table_name`.
  Этот путь не должен содержать префикс, заданный в конфигурации `<keeper_map_path_prefix>`, поскольку он будет автоматически добавлен к `root_path`.
  Также поддерживается формат `auxiliary_zookeeper_cluster_name:/some/path`, где `auxiliary_zookeeper_cluster` — это кластер ZooKeeper, определённый в конфигурации `<auxiliary_zookeepers>`.
  По умолчанию используется кластер ZooKeeper, определённый в конфигурации `<zookeeper>`.
* `keys_limit` — количество ключей, допустимое в таблице.
  Это мягкое ограничение, поэтому в некоторых редких случаях в таблице может оказаться больше ключей.
* `primary_key_name` – имя любого столбца из списка столбцов.
* `primary key` должен быть указан; поддерживается только один столбец в первичном ключе. Первичный ключ будет сериализован в бинарный вид как `имя узла` в ZooKeeper.
* столбцы, кроме первичного ключа, будут сериализованы в бинарный вид в соответствующем порядке и сохранены как значение итогового узла, определённого сериализованным ключом.
* запросы с фильтрацией по ключу через `equals` или `in` будут оптимизированы для поиска по нескольким ключам в `Keeper`, в противном случае будут получены все значения.

Пример:

```sql
CREATE TABLE keeper_map_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = KeeperMap('/keeper_map_table', 4)
PRIMARY KEY key
```

с

```xml
<clickhouse>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

Каждое значение, представляющее собой бинарную сериализацию `(v1, v2, v3)`, будет храниться по пути `/keeper_map_tables/keeper_map_table/data/serialized_key` в `Keeper`.
Кроме того, для количества ключей действует soft limit, равный 4.

Если несколько table созданы с одним и тем же путём в ZooKeeper, значения сохраняются до тех пор, пока существует хотя бы 1 table, использующая этот path.
В результате при создании table можно использовать предложение `ON CLUSTER` и организовать общий доступ к данным из нескольких экземпляров ClickHouse.
Разумеется, можно и вручную выполнить `CREATE TABLE` с тем же path в несвязанных экземплярах ClickHouse, чтобы добиться того же эффекта общего доступа к данным.

<div id="supported-operations">
  ## Поддерживаемые операции
</div>

<div id="inserts">
  ### Вставки
</div>

При вставке новых строк в `KeeperMap`, если ключ отсутствует, создается новая запись с этим ключом.
Если ключ уже существует и параметр `keeper_map_strict_mode` имеет значение `true`, генерируется исключение; в противном случае значение по этому ключу перезаписывается.

Пример:

```sql
INSERT INTO keeper_map_table VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### Удаление
</div>

Строки можно удалять с помощью запроса `DELETE` или `TRUNCATE`.
Если ключ существует и параметр `keeper_map_strict_mode` установлен в `true`, получение и удаление данных будут выполняться успешно только в том случае, если эти операции можно выполнить атомарно.

```sql
DELETE FROM keeper_map_table WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE keeper_map_table DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE keeper_map_table;
```

<div id="updates">
  ### Обновления
</div>

Значения можно изменять с помощью запроса `ALTER TABLE`. Первичный ключ изменять нельзя.
Если параметр `keeper_map_strict_mode` установлен в значение `true`, получение и обновление данных будут успешны только в том случае, если выполняются атомарно.

```sql
ALTER TABLE keeper_map_table UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Создание приложений для Real-time аналитики с ClickHouse и Hex](https://clickhouse.com/blog/building-real-time-applications-with-clickhouse-and-hex-notebook-keeper-engine)