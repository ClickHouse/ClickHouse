# ClickHouse Playground {#clickhouse-playground}

ClickHouse Playground позволяет моментально выполнить запросы к ClickHouse из бразуера.
В Playground доступны несколько тестовых массивов данных и примеры запросов, которые показывают некоторые отличительные черты ClickHouse.

Запросы выполняются под пользователем с правами `readonly` для которого есть следующие ограничения:
- запрещены DDL запросы
- запрещены INSERT запросы

Также установлены следующие опции:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouse Playground соответствует конфигурации m2.small хосту
[Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
запущеному в [Яндекс.Облаке](https://cloud.yandex.com/).
Больше информации про [облачных провайдерах](../commercial/cloud.md).

Веб интерфейс ClickHouse Playground делает запросы через ClickHouse HTTP API.
Бекендом служит обычный кластер ClickHouse.
ClickHouse HTTP интерфейс также доступен как часть Playground.

Запросы к Playground могут быть выполнены с помощью curl/wget, а также через соединеие JDBC/ODBC драйвера
Больше информации про приложения с поддержкой ClickHouse доступно в разделе [Интерфейсы](../interfaces/index.md).

| Параметр         | Значение                              |
|:-----------------|:--------------------------------------|
| Адрес            | https://play-api.clickhouse.tech:8443 |
| Имя пользователя | `playground`                          |
| Пароль           | `clickhouse`                          |

Требуется SSL соединение.

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
