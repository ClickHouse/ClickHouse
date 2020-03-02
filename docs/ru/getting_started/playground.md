#ClickHouse Playground

ClickHouse Playground позволяет моментально выполнить запросы к ClickHouse из бразуера.
В Playground доступны несколько тестовых массивов данных и примеры запросов, которые показывают некоторые отличительные черты ClickHouse.

Запросы выполняются под пользователем с правами `readonly` для которого есть следующие ограничения:
- запрещены DDL запросы
- запроещены INSERT запросы

ClickHouse Playground соответствует конфигурации m2.small хосту
[Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
запущеному в [Яндекс.Облаке](https://cloud.yandex.com/).
Больше информации про [облачных провайдерах](../commercial/cloud.md).

Веб интерфейс ClickHouse Playground делает запросы через ClickHouse HTTP API.
Бекендом служит обычный кластер ClickHouse.
ClickHouse HTTP интерфейс также доступен как часть Playground.

Запросы к Playground могут быть выполнены с помощью curl/wget, а также через соединеие JDBC/ODBC драйвера
Больше информации про приложения с поддержкой ClickHouse доступно в разделе [Интерфейсы](../interfaces/index.md).

| Параметр | Значение |  
|:----------|:-------------|
| Адрес| https://play-api.clickhouse.tech:8443 |
| Имя пользователя  | `playground`  |
| Пароль  | `clickhouse`  |

Требуется SSL соединение.

```bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
