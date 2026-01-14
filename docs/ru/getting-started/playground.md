---
slug: /ru/getting-started/playground
sidebar_position: 14
sidebar_label: Playground
---

# ClickHouse Playground {#clickhouse-playground}

[ClickHouse Playground](https://sql.clickhouse.com) позволяет пользователям экспериментировать с ClickHouse, выполняя запросы мгновенно, без необходимости настройки сервера или кластера.
В Playground доступны несколько примеров наборов данных.

Вы можете выполнять запросы к Playground, используя любой HTTP-клиент, например [curl](https://curl.haxx.se) или [wget](https://www.gnu.org/software/wget/), или настроить соединение, используя драйверы [JDBC](../interfaces/jdbc.md) или [ODBC](../interfaces/odbc.md). Дополнительную информацию о программных продуктах, поддерживающих ClickHouse, можно найти [здесь](../interfaces/index.md).

## Учетные данные {#credentials}

| Параметр            | Значение                           |
|:--------------------|:-----------------------------------|
| HTTPS-адрес         | `https://play.clickhouse.com:443/` |
| TCP-адрес           | `play.clickhouse.com:9440`         |
| Пользователь        | `explorer` или `play`              |
| Пароль              | (пусто)                            |

## Ограничения {#limitations}

Запросы выполняются от имени пользователя с правами только на чтение. Это предполагает некоторые ограничения:

-   DDL-запросы не разрешены
-   INSERT-запросы не разрешены

Сервис также имеет квоты на использование.

## Примеры {#examples}

Пример использования HTTPS-адреса с `curl`:

```bash
curl "https://play.clickhouse.com/?user=explorer" --data-binary "SELECT 'Play ClickHouse'"
```

Пример использования TCP-адреса с [CLI](../interfaces/cli.md):

``` bash
clickhouse client --secure --host play.clickhouse.com --user explorer
```
