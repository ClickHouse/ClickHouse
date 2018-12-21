# Разрешения для запросов {#permissions_for_queries}

Запросы в ClickHouse можно разделить на несколько типов:

1. Запросы на чтение данных: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
1. Запросы за запись данных: `INSERT`, `OPTIMIZE`.
1. Запросы на изменение настроек: `SET`, `USE`.
1. [Запросы DDL](https://ru.wikipedia.org/wiki/Data_Definition_Language): `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
1. `KILL QUERY`.

Разрешения пользователя по типу запроса регулируются параметрами:

- [readonly](#settings_readonly) — ограничивает разрешения для всех типов запросов, кроме DDL.
- [allow_ddl](#settings_allow_ddl) — ограничивает разрешения для DDL запросов.

`KILL QUERY` выполняется с любыми настройками.

## readonly {#settings_readonly}

Ограничивает разрешения для запросов на чтение данных, запись данных и изменение параметров.

Разделение запросов по типам смотрите по тексту [выше](#permissions_for_queries) по тексту.

**Возможные значения**

- 0 — разрешены все запросы.
- 1 — разрешены только запросы на чтение данных.
- 2 — разрешены запросы на чтение данных и изменение настроек.

После установки `readonly = 1`пользователь не может изменить настройки `readonly` и `allow_ddl` в текущей сессии.

При использовании метода `GET`  в [HTTP интерфейсе](../../interfaces/http.md#http_interface), `readonly = 1` устанавливается автоматически. Для изменения данных используйте метод `POST`.

**Значение по умолчанию**

0

## allow_ddl {#settings_allow_ddl}

Разрешает/запрещает [DDL](https://ru.wikipedia.org/wiki/Data_Definition_Language) запросы.

Разделение запросов по типам смотрите по тексту [выше](#permissions_for_queries) по тексту.

**Возможные значения**

- 0 — DDL запросы не разрешены.
- 1 — DDL запросы разрешены.

Если `allow_ddl = 0`, то невозможно выполнить `SET allow_ddl = 1`  для текущей сессии.

**Значение по умолчанию**

1

[Оригинальная статья](https://clickhouse.yandex/docs/ru/operations/settings/permissions_for_queries/) <!--hide-->
