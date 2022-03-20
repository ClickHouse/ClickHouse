---
toc_priority: 14
toc_title: Playground
---

# ClickHouse Playground {#clickhouse-playground}

!!! warning "Warning"
    This service is deprecated and will be replaced in foreseeable future.

[ClickHouse Playground](https://play.clickhouse.com) позволяет пользователям экспериментировать с ClickHouse, мгновенно выполняя запросы без настройки своего сервера или кластера.
В Playground доступны несколько тестовых массивов данных, а также примеры запросов, которые показывают возможности ClickHouse. Кроме того, вы можете выбрать LTS релиз ClickHouse, который хотите протестировать.

Вы можете отправлять запросы к Playground с помощью любого HTTP-клиента, например [curl](https://curl.haxx.se) или [wget](https://www.gnu.org/software/wget/), также можно установить соединение с помощью драйверов [JDBC](../interfaces/jdbc.md) или [ODBC](../interfaces/odbc.md). Более подробная информация о программных продуктах, поддерживающих ClickHouse, доступна [здесь](../interfaces/index.md).

## Параметры доступа {#credentials}

| Параметр            | Значение                                |
|:--------------------|:----------------------------------------|
| Конечная точка HTTPS| `https://play-api.clickhouse.com:8443` |
| Конечная точка TCP  | `play-api.clickhouse.com:9440`         |
| Пользователь        | `playground`                            |
| Пароль              | `clickhouse`                            |

Также можно подключаться к ClickHouse определённых релизов, чтобы протестировать их различия (порты и пользователь / пароль остаются неизменными):

-   20.3 LTS: `play-api-v20-3.clickhouse.com`
-   19.14 LTS: `play-api-v19-14.clickhouse.com`

!!! note "Примечание"
    Для всех этих конечных точек требуется безопасное соединение TLS.

## Ограничения {#limitations}

Запросы выполняются под пользователем с правами `readonly`, для которого есть следующие ограничения:
- запрещены DDL запросы
- запрещены INSERT запросы

Также установлены следующие опции:
- [max_result_bytes=10485760](../operations/settings/query-complexity.md#max-result-bytes)
- [max_result_rows=2000](../operations/settings/query-complexity.md#setting-max_result_rows)
- [result_overflow_mode=break](../operations/settings/query-complexity.md#result-overflow-mode)
- [max_execution_time=60000](../operations/settings/query-complexity.md#max-execution-time)

## Примеры {#examples}

Пример конечной точки HTTPS с `curl`:

``` bash
curl "https://play-api.clickhouse.com:8443/?query=SELECT+'Play+ClickHouse\!';&user=playground&password=clickhouse&database=datasets"
```

Пример конечной точки TCP с [CLI](../interfaces/cli.md):

``` bash
clickhouse client --secure -h play-api.clickhouse.com --port 9440 -u playground --password clickhouse -q "SELECT 'Play ClickHouse\!'"
```
