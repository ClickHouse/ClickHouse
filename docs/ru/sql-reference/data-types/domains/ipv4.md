## IPv4 {#ipv4}

`IPv4` — это домен, базирующийся на типе данных `UInt32` предназначенный для хранения адресов IPv4. Он обеспечивает компактное хранение данных с удобным для человека форматом ввода-вывода, и явно отображаемым типом данных в структуре таблицы.

### Применение {#primenenie}

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

Или вы можете использовать домен IPv4 в качестве ключа:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` поддерживает вставку в виде строк с текстовым представлением IPv4 адреса:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.tech', '183.247.232.58')('https://clickhouse.tech/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.tech/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.tech          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

Значения хранятся в компактной бинарной форме:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

Значения с доменным типом данных не преобразуются неявно в другие типы данных, кроме `UInt32`.
Если необходимо преобразовать значение типа `IPv4` в строку, то это необходимо делать явно с помощью функции `IPv4NumToString()`:

``` sql
SELECT toTypeName(s), IPv4NumToString(from) AS s FROM hits LIMIT 1;
```

``` text
┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
│ String                            │ 183.247.232.58 │
└───────────────────────────────────┴────────────────┘
```

Или приводить к типу данных `UInt32`:

``` sql
SELECT toTypeName(i), CAST(from AS UInt32) AS i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/data_types/domains/ipv4) <!--hide-->
