<a name="table_engines-mysql"></a>

# MySQL

Движок MySQL позволяет выполнять `SELECT` запросы над данными, хранящимися на удалённом MySQL сервере.

Формат вызова:

```
MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**Параметры вызова**

- `host:port` — Адрес сервера MySQL.
- `database` — Имя базы данных на сервере MySQL.
- `table` — Имя таблицы.
- `user` — Пользователь MySQL.
- `password` — Пароль пользователя.
- `replace_query` — Флаг, устанавливающий замену запроса `INSERT INTO` на `REPLACE INTO`. Если `replace_query=1`, то запрос заменяется.
- `'on_duplicate_clause'` — Добавляет выражение `ON DUPLICATE KEY UPDATE 'on_duplicate_clause'` в запрос `INSERT`. Например, `impression = VALUES(impression) + impression`. Чтобы указать `'on_duplicate_clause'` необходимо передать `0` в параметр `replace_query`. Если одновременно передать `replace_query = 1` и `'on_duplicate_clause'`, то ClickHouse сгенерирует исключение.

На данный момент простые условия `WHERE`, такие как `=, !=, >, >=, <, <=` будут выполняться на стороне сервера MySQL.

Остальные условия и ограничение выборки `LIMIT` будут выполнены в ClickHouse только после выполнения запроса к MySQL.

Движок `MySQL` не поддерживает тип данных [Nullable](../../data_types/nullable.md#data_type-nullable), поэтому при чтении данных из таблиц MySQL `NULL` преобразуются в значения по умолчанию для заданного типа столбца, обычно это 0 или пустая строка.
