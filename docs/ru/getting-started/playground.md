---
toc_priority: 14
toc_title: Playground
---

# ClickHouse Playground {#clickhouse-playground}

[ClickHouse Playground](https://play.clickhouse.tech) позволяет пользователям экспериментировать с ClickHouse, мгновенно выполняя запросы без настройки своего сервера или кластера.
В Playground доступны несколько тестовых массивов данных, а также примеры запросов, которые показывают возможности ClickHouse. Кроме того, вы можете выбрать LTS релиз ClickHouse, который хотите протестировать.

ClickHouse Playground дает возможность поработать с  [Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse) в конфигурации m2.small (4 vCPU, 32 ГБ ОЗУ), которую предосталяет [Яндекс.Облако](https://cloud.yandex.com/). Дополнительную информацию об облачных провайдерах читайте в разделе [Поставщики облачных услуг ClickHouse](../commercial/cloud.md).

Вы можете отправлять запросы к Playground с помощью любого HTTP-клиента, например [curl](https://curl.haxx.se) или [wget](https://www.gnu.org/software/wget/), также можно установить соединение с помощью драйверов [JDBC](../interfaces/jdbc.md) или [ODBC](../interfaces/odbc.md). Более подробная информация о программных продуктах, поддерживающих ClickHouse, доступна [здесь](../interfaces/index.md).

## Параметры доступа {#credentials}

| Параметр            | Значение                                |
|:--------------------|:----------------------------------------|
| Конечная точка HTTPS| `https://play-api.clickhouse.tech:8443` |
| Конечная точка TCP  | `play-api.clickhouse.tech:9440`         |
| Пользователь        | `playground`                            |
| Пароль              | `clickhouse`                            |

Также можно подключаться к ClickHouse определённых релизов, чтобы протестировать их различия (порты и пользователь / пароль остаются неизменными):

-   20.3 LTS: `play-api-v20-3.clickhouse.tech`
-   19.14 LTS: `play-api-v19-14.clickhouse.tech`

!!! note "Примечание"
    Для всех этих конечных точек требуется безопасное соединение TLS.

## Ограничения {#limitations}

Запросы выполняются под пользователем с правами `readonly`, для которого есть следующие ограничения:
- запрещены DDL запросы
- запрещены INSERT запросы

Также установлены следующие опции:
- [max_result_bytes=10485760](../operations/settings/query_complexity/#max-result-bytes)
- [max_result_rows=2000](../operations/settings/query_complexity/#setting-max_result_rows)
- [result_overflow_mode=break](../operations/settings/query_complexity/#result-overflow-mode)
- [max_execution_time=60000](../operations/settings/query_complexity/#max-execution-time)

## Примеры {#examples}

Пример конечной точки HTTPS с `curl`:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse\!';&user=playground&password=clickhouse&database=datasets"
```

Пример конечной точки TCP с [CLI](../interfaces/cli.md):

``` bash
clickhouse client --secure -h play-api.clickhouse.tech --port 9440 -u playground --password clickhouse -q "SELECT 'Play ClickHouse\!'"
```

## Детали реализации {#implementation-details}

Веб-интерфейс ClickHouse Playground выполняет запросы через ClickHouse [HTTP API](../interfaces/http.md).
Бэкэнд Playground - это кластер ClickHouse без дополнительных серверных приложений. Как упоминалось выше,  способы подключения по HTTPS и TCP/TLS общедоступны как часть Playground. Они проксируются через [Cloudflare Spectrum](https://www.cloudflare.com/products/cloudflare-spectrum/) для добавления дополнительного уровня защиты и улучшенного глобального подключения.

!!! warning "Предупреждение"
Открывать сервер ClickHouse для публичного доступа  в любой другой ситуации **настоятельно не рекомендуется**. Убедитесь, что он настроен только на частную сеть и защищен брандмауэром.
