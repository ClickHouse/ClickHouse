---
description: 'Документация по KILL'
sidebar_label: 'KILL'
sidebar_position: 46
slug: /sql-reference/statements/kill
title: 'Команды KILL'
doc_type: 'reference'
---

Существует два вида команд KILL: для завершения запроса и для завершения мутации

<div id="kill-query">
  ## KILL QUERY
</div>

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Используется для принудительного завершения выполняющихся в данный момент запросов.
Запросы для завершения выбираются из таблицы system.processes по критериям, заданным в предложении `WHERE` запроса `KILL`.

Примеры:

Сначала нужно получить список незавершённых запросов. Этот SQL-запрос выводит их в порядке от самых длительно выполняющихся:

Список с одного узла ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Список в кластере ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Завершите запрос:

```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

:::tip
Если вы завершаете запрос в ClickHouse Cloud или в самоуправляемом кластере, обязательно используйте опцию `ON CLUSTER [cluster-name]`, чтобы запрос был завершён на всех репликах
:::

Пользователи с правами только для чтения могут останавливать только собственные запросы.

По умолчанию используется асинхронная версия запросов (`ASYNC`), которая не ждёт подтверждения того, что запросы остановлены.

Синхронная версия (`SYNC`) дожидается остановки всех запросов и отображает информацию о каждом процессе по мере его завершения.
Ответ содержит столбец `kill_status`, который может принимать следующие значения:

1. `finished` – запрос был успешно завершён.
2. `waiting` – ожидание завершения запроса после отправки ему сигнала на завершение.
3. Другие значения объясняют, почему запрос невозможно остановить.

Тестовый запрос (`TEST`) только проверяет права пользователя и показывает список запросов, которые будут остановлены.

<div id="kill-mutation">
  ## KILL MUTATION
</div>

Наличие длительно выполняющихся или незавершённых мутаций часто указывает на то, что сервис ClickHouse работает нестабильно. Асинхронная природа мутаций может приводить к тому, что они потребляют все доступные ресурсы системы. Вам может потребоваться:

* Приостановить все новые мутации, `INSERT`ы и `SELECT`ы и дождаться завершения очереди мутаций.
* Или вручную завершить некоторые из этих мутаций, отправив команду `KILL`.

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Пытается отменить и удалить [мутации](/ru/sql-reference/statements/alter#mutations), которые сейчас выполняются. Мутации для отмены выбираются из таблицы [`system.mutations`](/ru/operations/system-tables/mutations) с помощью фильтра, указанного в предложении `WHERE` запроса `KILL`.

Тестовый запрос (`TEST`) только проверяет права пользователя и выводит список мутаций, которые нужно остановить.

Примеры:

Получить `count()` незавершённых мутаций:

Количество мутаций на одном узле ClickHouse:

```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

Количество мутаций в кластере реплик ClickHouse:

```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Выведите список незавершённых мутаций:

Список мутаций на одном узле ClickHouse:

```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

Список мутаций в кластере ClickHouse:

```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

При необходимости завершите мутации:

```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

Запрос полезен, когда мутация зависла и не может завершиться (например, если какая-то функция в запросе мутации генерирует исключение при применении к данным в таблице).

Изменения, уже внесённые мутацией, не откатываются.

:::note
Столбец `is_killed=1` (Только в ClickHouse Cloud) в таблице [system.mutations](/ru/operations/system-tables/mutations) не обязательно означает, что мутация полностью завершена. Мутация может оставаться в состоянии, когда `is_killed=1` и `is_done=0`, в течение длительного времени. Это может происходить, если другая долго выполняющаяся мутация блокирует остановленную мутацию. Это нормальная ситуация.
:::