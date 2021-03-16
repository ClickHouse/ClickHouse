---
toc_priority: 46
toc_title: KILL
---

# KILL {#kill-statements}

Существует два вида операторов KILL: KILL QUERY и KILL MUTATION

## KILL QUERY {#kill-query}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Пытается принудительно остановить исполняющиеся в данный момент запросы.
Запросы для принудительной остановки выбираются из таблицы system.processes с помощью условия, указанного в секции `WHERE` запроса `KILL`.

Примеры

``` sql
-- Принудительно останавливает все запросы с указанным query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Синхронно останавливает все запросы пользователя 'username':
KILL QUERY WHERE user='username' SYNC
```

Readonly-пользователи могут останавливать только свои запросы.

По умолчанию используется асинхронный вариант запроса (`ASYNC`), который не дожидается подтверждения остановки запросов.

Синхронный вариант (`SYNC`) ожидает остановки всех запросов и построчно выводит информацию о процессах по ходу их остановки.
Ответ содержит колонку `kill_status`, которая может принимать следующие значения:

1.  ‘finished’ - запрос был успешно остановлен;
2.  ‘waiting’ - запросу отправлен сигнал завершения, ожидается его остановка;
3.  остальные значения описывают причину невозможности остановки запроса.

Тестовый вариант запроса (`TEST`) только проверяет права пользователя и выводит список запросов для остановки.

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Пытается остановить выполняющиеся в данные момент [мутации](alter/index.md#mutations). Мутации для остановки выбираются из таблицы [`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations) с помощью условия, указанного в секции `WHERE` запроса `KILL`.

Тестовый вариант запроса (`TEST`) только проверяет права пользователя и выводит список запросов для остановки.

Примеры:

``` sql
-- Останавливает все мутации одной таблицы:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Останавливает конкретную мутацию:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

Запрос полезен в случаях, когда мутация не может выполниться до конца (например, если функция в запросе мутации бросает исключение на данных таблицы).

Данные, уже изменённые мутацией, остаются в таблице (отката на старую версию данных не происходит).


[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/kill/) <!--hide-->