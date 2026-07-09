---
description: 'Эта табличная функция позволяет интегрировать ClickHouse с Redis.'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

Эта табличная функция позволяет интегрировать ClickHouse с [Redis](https://redis.io/).

<div id="syntax">
  ## Синтаксис
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## Аргументы
</div>

| Аргумент    | Описание                                                                                                                                       |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port` | Адрес сервера Redis; порт можно не указывать — тогда будет использован стандартный порт Redis `6379`.                                          |
| `key`       | Любое имя столбца из списка столбцов.                                                                                                          |
| `structure` | Схема таблицы ClickHouse, возвращаемой этой функцией.                                                                                          |
| `db_index`  | Индекс БД Redis в диапазоне от 0 до 15; значение по умолчанию — 0.                                                                             |
| `password`  | Пароль пользователя; по умолчанию — пустая строка.                                                                                             |
| `pool_size` | Максимальный размер пула соединений Redis; значение по умолчанию — 16.                                                                         |
| `primary`   | Обязательный параметр; поддерживается только один столбец в первичном ключе. Первичный ключ будет сериализован в двоичном виде как ключ Redis. |

* столбцы, кроме первичного ключа, будут сериализованы в двоичном виде как значение Redis в соответствующем порядке.
* запросы с фильтрацией по `key` через `equals` или `in` будут оптимизированы в выборку по нескольким ключам из Redis. Если запрос выполняется без фильтрации по `key`, произойдет полное сканирование таблицы, что является ресурсоемкой операцией.

[Именованные коллекции](/ru/operations/named-collections.md) в настоящее время не поддерживаются для табличной функции `redis`.

<div id="returned_value">
  ## Возвращаемое значение
</div>

Объект table, в котором ключ используется как ключ Redis, а остальные столбцы объединяются в значение Redis.

<div id="usage-example">
  ## Пример использования
</div>

Чтение из Redis:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Вставка в Redis:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## См. также
</div>

* [Движок таблицы `Redis`](/ru/engines/table-engines/integrations/redis.md)
* [Использование Redis в качестве источника словаря](/ru/sql-reference/statements/create/dictionary/sources/redis)