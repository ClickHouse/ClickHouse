---
description: 'Позволяет получать доступ ко всем сегментам (настроенным в разделе `remote_servers`)
  кластера без создания таблицы [Distributed](../../engines/table-engines/special/distributed.md).'
sidebar_label: 'cluster'
sidebar_position: 30
slug: /sql-reference/table-functions/cluster
title: 'clusterAllReplicas'
doc_type: 'reference'
---

Позволяет получать доступ ко всем сегментам (настроенным в разделе `remote_servers`) кластера без создания таблицы [Distributed](../../engines/table-engines/special/distributed.md). Для каждого сегмента запрашивается только одна реплика.

Функция `clusterAllReplicas` — то же, что и `cluster`, но запрашиваются все реплики. Каждая реплика в кластере рассматривается как отдельный сегмент/соединение.

:::note
Все доступные кластеры перечислены в таблице [system.clusters](../../operations/system-tables/clusters.md).
:::

<div id="syntax">
  ## Синтаксис
</div>

```sql
cluster(['cluster_name', db.table, sharding_key])
cluster(['cluster_name', db, table, sharding_key])
clusterAllReplicas(['cluster_name', db.table, sharding_key])
clusterAllReplicas(['cluster_name', db, table, sharding_key])
```

<div id="arguments">
  ## Аргументы
</div>

| Аргументы                    | Тип                                                                                                                                                            |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`               | Имя кластера, используемое для формирования набора адресов и параметров подключения к удалённым и локальным серверам; если не указано, используется `default`. |
| `db.table` или `db`, `table` | Имя базы данных и таблицы.                                                                                                                                     |
| `sharding_key`               | Ключ сегментирования. Необязательный параметр. Его необходимо указать, если в кластере более одного сегмента.                                                  |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Данные из кластеров.

<div id="using_macros">
  ## Использование макросов
</div>

`cluster_name` может содержать макросы — подстановки в `{}`. Подставляемое значение берётся из раздела [macros](../../operations/server-configuration-parameters/settings.md#macros) в файле конфигурации сервера.

Пример:

```sql
SELECT * FROM cluster('{cluster}', default.example_table);
```

<div id="usage_recommendations">
  ## Использование и рекомендации
</div>

Использование табличных функций `cluster` и `clusterAllReplicas` менее эффективно, чем создание таблицы `Distributed`, поскольку в этом случае соединение с сервером заново устанавливается для каждого запроса. При обработке большого количества запросов всегда заранее создавайте таблицу `Distributed` и не используйте табличные функции `cluster` и `clusterAllReplicas`.

Табличные функции `cluster` и `clusterAllReplicas` могут быть полезны в следующих случаях:

* Доступ к определённому кластеру для сравнения данных, отладки и тестирования.
* Запросы к различным кластерам ClickHouse и репликам в исследовательских целях.
* Нечастые распределённые запросы, выполняемые вручную.

Параметры подключения, такие как `host`, `port`, `user`, `password`, `compression`, `secure`, берутся из раздела конфигурации `<remote_servers>`. Подробности см. в разделе [движок Distributed](../../engines/table-engines/special/distributed.md).

<div id="related">
  ## См. также
</div>

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [load&#95;balancing](../../operations/settings/settings.md#load_balancing)