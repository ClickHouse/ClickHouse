# Запросы SYSTEM {#query_language-system}

- [RELOAD DICTIONARIES](#query_language-system-reload-dictionaries)
- [RELOAD DICTIONARY](#query_language-system-reload-dictionary)
- [DROP DNS CACHE](#query_language-system-drop-dns-cache)
- [DROP MARKS CACHE](#query_language-system-drop-marks-cache)
- [FLUSH LOGS](#query_language-system-flush_logs)
- [RELOAD CONFIG](#query_language-system-reload-config)
- [SHUTDOWN](#query_language-system-shutdown)
- [KILL](#query_language-system-kill)
- [STOP DISTRIBUTED SENDS](#query_language-system-stop-distributed-sends)
- [FLUSH DISTRIBUTED](#query_language-system-flush-distributed)
- [START DISTRIBUTED SENDS](#query_language-system-start-distributed-sends)

## RELOAD DICTIONARIES {#query_language-system-reload-dictionaries}

Перегружает все словари, которые были успешно загружены до этого. 
По умолчанию включена ленивая загрузка [dictionaries_lazy_load](../operations/server_settings/settings.md#dictionaries-lazy-load), поэтому словари не загружаются автоматически при старте, а только при первом обращении через dictGet или SELECT к ENGINE=Dictionary. После этого такие словари (LOADED) будут перегружаться командой `system reload dictionaries`.
Всегда возвращает `Ok.`, вне зависимости от результата обновления словарей.

## RELOAD DICTIONARY dictionary_name {#query_language-system-reload-dictionary}

Полностью перегружает словарь `dictionary_name`, вне зависимости от состояния словаря (LOADED/NOT_LOADED/FAILED).
Всегда возвращает `Ok.`, вне зависимости от результата обновления словаря.
Состояние словаря можно проверить запросом к `system.dictionaries`.

```sql
SELECT name, status FROM system.dictionaries;
```

## DROP DNS CACHE {#query_language-system-drop-dns-cache}

Сбрасывает внутренний DNS кеш ClickHouse. Иногда (для старых версий ClickHouse) необходимо использовать эту команду при изменении инфраструктуры (смене IP адреса у другого ClickHouse сервера или сервера, используемого словарями).

Для более удобного (автоматического) управления кешем см. параметры disable_internal_dns_cache, dns_cache_update_period.

## DROP MARKS CACHE {#query_language-system-drop-marks-cache}

Сбрасывает кеш "засечек" (`mark cache`). Используется при разработке ClickHouse и тестах производительности.

## FLUSH LOGS {#query_language-system-flush_logs}

Записывает буферы логов в системные таблицы (например system.query_log). Позволяет не ждать 7.5 секунд при отладке.

## RELOAD CONFIG {#query_language-system-reload-config}

Перечитывает конфигурацию настроек ClickHouse. Используется при хранении конфигурации в zookeeeper.

## SHUTDOWN {#query_language-system-shutdown}

Штатно завершает работу ClickHouse (аналог `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

## KILL {#query_language-system-kill}

Аварийно завершает работу ClickHouse (аналог `kill -9 {$pid_clickhouse-server}`)

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

