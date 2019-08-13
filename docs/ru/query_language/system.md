# Запросы SYSTEM {#query_language-system}

- [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
- [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
- [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)

## Управление распределёнными таблицами {#query_language-system-distributed}

ClickHouse может оперировать [распределёнными](../operations/table_engines/distributed.md) таблицами. Когда пользователь вставляет данные в эти таблицы, ClickHouse сначала формирует очередь из данных, которые должны быть отправлены на узлы кластера, а затем асинхронно отправляет подготовленные данные. Вы пожете управлять очередью с помощью запросов [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends), [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends) и [FLUSH DISTRIBUTED](#query_language-system-flush-distributed). Также есть возможность синхронно вставлять распределенные данные с помощью настройки `insert_distributed_sync`.

### STOP DISTRIBUTED SENDS {#query_language-system-stop-distributed-sends}

Отключает фоновую отправку при вставке данных в распределённые таблицы.

```
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name>
```

### FLUSH DISTRIBUTED {#query_language-system-flush-distributed}

В синхронном режиме отправляет все данные на узлы кластера. Если какие-либо узлы недоступны, ClickHouse генерирует исключение и останавливает выполнение запроса. Такой запрос можно повторять до успешного завершения, что будет означать возвращение связанности с остальными узлами кластера.

```
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name>
```

### START DISTRIBUTED SENDS {#query_language-system-start-distributed-sends}

Включает фоновую отправку при вставке данных в распределенные таблицы.

```
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name>
```

[Оригинальная статья](https://clickhouse.yandex/docs/ru/query_language/system/) <!--hide-->

