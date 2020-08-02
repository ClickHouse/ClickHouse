---
toc_priority: 35
toc_title: CREATE
---

## CREATE DATABASE {#query-language-create-database}

Создает базу данных.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### Секции {#sektsii}

-   `IF NOT EXISTS`

        Если база данных с именем `db_name` уже существует, то ClickHouse не создаёт базу данных и:
        - Не генерирует исключение, если секция указана.
        - Генерирует исключение, если секция не указана.

-   `ON CLUSTER`

        ClickHouse создаёт базу данных `db_name` на всех серверах указанного кластера.

-   `ENGINE`

        - [MySQL](../../sql_reference/statements/create.md)

            Позволяет получать данные с удаленного сервера MySQL.

        По умолчанию ClickHouse использует собственный [движок баз данных](../../sql_reference/statements/create.md).

## CREATE TABLE {#create-table-query}

Запрос `CREATE TABLE` может иметь несколько форм.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

Создаёт таблицу с именем name в БД db или текущей БД, если db не указана, со структурой, указанной в скобках, и движком engine.
Структура таблицы представляет список описаний столбцов. Индексы, если поддерживаются движком, указываются в качестве параметров для движка таблицы.

Описание столбца, это `name type`, в простейшем случае. Пример: `RegionID UInt32`.
Также могут быть указаны выражения для значений по умолчанию - смотрите ниже.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

Создаёт таблицу с такой же структурой, как другая таблица. Можно указать другой движок для таблицы. Если движок не указан, то будет выбран такой же движок, как у таблицы `db2.name2`.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Создаёт таблицу с такой же структурой и данными, как результат соответствующей табличной функцией.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

Создаёт таблицу со структурой, как результат запроса `SELECT`, с движком engine, и заполняет её данными из SELECT-а.

Во всех случаях, если указано `IF NOT EXISTS`, то запрос не будет возвращать ошибку, если таблица уже существует. В этом случае, запрос будет ничего не делать.

После секции `ENGINE` в запросе могут использоваться и другие секции в зависимости от движка. Подробную документацию по созданию таблиц смотрите в описаниях [движков таблиц](../../sql-reference/statements/create.md#table_engines).

### Значения по умолчанию {#create-default-values}

В описании столбца, может быть указано выражение для значения по умолчанию, одного из следующих видов:
`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
Пример: `URLDomain String DEFAULT domain(URL)`.

Если выражение для значения по умолчанию не указано, то в качестве значений по умолчанию будут использоваться нули для чисел, пустые строки для строк, пустые массивы для массивов, а также `1970-01-01` для дат и нулевой unix timestamp для дат с временем, NULL-ы для Nullable.

В случае, если указано выражение по умолчанию, то указание типа столбца не обязательно. При отсутствии явно указанного типа, будет использован тип выражения по умолчанию. Пример: `EventDate DEFAULT toDate(EventTime)` - для столбца EventDate будет использован тип Date.

При наличии явно указанного типа данных и выражения по умолчанию, это выражение будет приводиться к указанному типу с использованием функций приведения типа. Пример: `Hits UInt32 DEFAULT 0` - имеет такой же смысл, как `Hits UInt32 DEFAULT toUInt32(0)`.

В качестве выражения для умолчания, может быть указано произвольное выражение от констант и столбцов таблицы. При создании и изменении структуры таблицы, проверяется, что выражения не содержат циклов. При INSERT-е проверяется разрешимость выражений - что все столбцы, из которых их можно вычислить, переданы.

`DEFAULT expr`

Обычное значение по умолчанию. Если в запросе INSERT не указан соответствующий столбец, то он будет заполнен путём вычисления соответствующего выражения.

`MATERIALIZED expr`

Материализованное выражение. Такой столбец не может быть указан при INSERT, то есть, он всегда вычисляется.
При INSERT без указания списка столбцов, такие столбцы не рассматриваются.
Также этот столбец не подставляется при использовании звёздочки в запросе SELECT. Это необходимо, чтобы сохранить инвариант, что дамп, полученный путём `SELECT *`, можно вставить обратно в таблицу INSERT-ом без указания списка столбцов.

`ALIAS expr`

Синоним. Такой столбец вообще не хранится в таблице.
Его значения не могут быть вставлены в таблицу, он не подставляется при использовании звёздочки в запросе SELECT.
Он может быть использован в SELECT-ах - в таком случае, во время разбора запроса, алиас раскрывается.

При добавлении новых столбцов с помощью запроса ALTER, старые данные для этих столбцов не записываются. Вместо этого, при чтении старых данных, для которых отсутствуют значения новых столбцов, выполняется вычисление выражений по умолчанию на лету. При этом, если выполнение выражения требует использования других столбцов, не указанных в запросе, то эти столбцы будут дополнительно прочитаны, но только для тех блоков данных, для которых это необходимо.

Если добавить в таблицу новый столбец, а через некоторое время изменить его выражение по умолчанию, то используемые значения для старых данных (для данных, где значения не хранились на диске) поменяются. Также заметим, что при выполнении фоновых слияний, данные для столбцов, отсутствующих в одном из сливаемых кусков, записываются в объединённый кусок.

Отсутствует возможность задать значения по умолчанию для элементов вложенных структур данных.

### Ограничения (constraints) {#constraints}

Наряду с объявлением столбцов можно объявить ограничения на значения в столбцах таблицы:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` может быть любым булевым выражением, состоящим из операторов сравнения или функций. При наличии одного или нескольких ограничений в момент вставки данных выражения ограничений будут проверяться на истинность для каждой вставляемой строки данных. В случае, если в теле INSERT запроса придут некорректные данные — клиент получит исключение с описанием нарушенного ограничения.

Добавление большого числа ограничений может негативно повлиять на производительность `INSERT` запросов.

### Выражение для TTL {#vyrazhenie-dlia-ttl}

Определяет время хранения значений. Может быть указано только для таблиц семейства MergeTree. Подробнее смотрите в [TTL для столбцов и таблиц](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### Кодеки сжатия столбцов {#codecs}

По умолчанию, ClickHouse применяет к столбцу метод сжатия, определённый в [конфигурации сервера](../../sql-reference/statements/create.md#compression). Кроме этого, можно задать метод сжатия для каждого отдельного столбца в запросе `CREATE TABLE`.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

Если задать кодек для столбца, то кодек по умолчанию не применяется. Кодеки можно последовательно комбинировать, например, `CODEC(Delta, ZSTD)`. Чтобы выбрать наиболее подходящую для вашего проекта комбинацию кодеков, необходимо провести сравнительные тесты, подобные тем, что описаны в статье Altinity [New Encodings to Improve ClickHouse Efficiency](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse).

!!! warning "Предупреждение"
    Нельзя распаковать базу данных ClickHouse с помощью сторонних утилит наподобие `lz4`. Необходимо использовать специальную утилиту [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor).

Сжатие поддерживается для следующих движков таблиц:

-   [MergeTree family](../../sql-reference/statements/create.md)
-   [Log family](../../sql-reference/statements/create.md)
-   [Set](../../sql-reference/statements/create.md)
-   [Join](../../sql-reference/statements/create.md)

ClickHouse поддерживает кодеки общего назначения и специализированные кодеки.

#### Специализированные кодеки {#create-query-specialized-codecs}

Эти кодеки разработаны для того, чтобы, используя особенности данных сделать сжатие более эффективным. Некоторые из этих кодеков не сжимают данные самостоятельно. Они готовят данные для кодеков общего назначения, которые сжимают подготовленные данные эффективнее, чем неподготовленные.

Специализированные кодеки:

-   `Delta(delta_bytes)` — Метод, в котором исходные значения заменяются разностью двух соседних значений, за исключением первого значения, которое остаётся неизменным. Для хранения разниц используется до `delta_bytes`, т.е. `delta_bytes` — это максимальный размер исходных данных. Возможные значения `delta_bytes`: 1, 2, 4, 8. Значение по умолчанию для `delta_bytes` равно `sizeof(type)`, если результат 1, 2, 4, or 8. Во всех других случаях — 1.
-   `DoubleDelta` — Вычисляется разницу от разниц и сохраняет её в компакном бинарном виде. Оптимальная степень сжатия достигается для монотонных последовательностей с постоянным шагом, наподобие временных рядов. Можно использовать с любым типом данных фиксированного размера. Реализует алгоритм, используемый в TSDB Gorilla, поддерживает 64-битные типы данных. Использует 1 дополнительный бит для 32-байтовых значений: 5-битные префиксы вместо 4-битных префиксов. Подробнее читайте в разделе «Compressing Time Stamps» документа [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Вычисляет XOR между текущим и предыдущим значением и записывает результат в компактной бинарной форме. Еффективно сохраняет ряды медленно изменяющихся чисел с плавающей запятой, поскольку наилучший коэффициент сжатия достигается, если соседние значения одинаковые. Реализует алгоритм, используемый в TSDB Gorilla, адаптируя его для работы с 64-битными значениями. Подробнее читайте в разделе «Compressing Values» документа [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Метод сжатия который обрезает неиспользуемые старшие биты целочисленных значений (включая `Enum`, `Date` и `DateTime`). На каждом шаге алгоритма, кодек помещает блок из 64 значений в матрицу 64✕64, транспонирует её, обрезает неиспользуемые биты, а то, что осталось возвращает в виде последовательности. Неиспользуемые биты, это биты, которые не изменяются от минимального к максимальному на всём диапазоне значений куска данных.

Кодеки `DoubleDelta` и `Gorilla` используются в TSDB Gorilla как компоненты алгоритма сжатия. Подход Gorilla эффективен в сценариях, когда данные представляют собой медленно изменяющиеся во времени величины. Метки времени эффективно сжимаются кодеком `DoubleDelta`, а значения кодеком `Gorilla`. Например, чтобы создать эффективно хранящуюся таблицу, используйте следующую конфигурацию:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### Кодеки общего назначения {#create-query-common-purpose-codecs}

Кодеки:

-   `NONE` — без сжатия.
-   `LZ4` — [алгоритм сжатия без потерь](https://github.com/lz4/lz4) используемый по умолчанию. Применяет быстрое сжатие LZ4.
-   `LZ4HC[(level)]` — алгоритм LZ4 HC (high compression) с настраиваемым уровнем сжатия. Уровень по умолчанию — 9. Настройка `level <= 0` устанавливает уровень сжания по умолчанию. Возможные уровни сжатия: \[1, 12\]. Рекомендуемый диапазон уровней: \[4, 9\].
-   `ZSTD[(level)]` — [алгоритм сжатия ZSTD](https://en.wikipedia.org/wiki/Zstandard) с настраиваемым уровнем сжатия `level`. Возможные уровни сжатия: \[1, 22\]. Уровень сжатия по умолчанию: 1.

Высокие уровни сжатия полезны для ассимметричных сценариев, подобных «один раз сжал, много раз распаковал». Высокие уровни сжатия подразумеваю лучшее сжатие, но большее использование CPU.

## Временные таблицы {#vremennye-tablitsy}

ClickHouse поддерживает временные таблицы со следующими характеристиками:

-   Временные таблицы исчезают после завершения сессии, в том числе при обрыве соединения.
-   Временная таблица использует только модуль памяти.
-   Невозможно указать базу данных для временной таблицы. Она создается вне баз данных.
-   Невозможно создать временную таблицу распределнным DDL запросом на всех серверах кластера (с опцией `ON CLUSTER`): такая таблица существует только в рамках существующей сессии.
-   Если временная таблица имеет то же имя, что и некоторая другая, то, при упоминании в запросе без указания БД, будет использована временная таблица.
-   При распределённой обработке запроса, используемые в запросе временные таблицы, передаются на удалённые серверы.

Чтобы создать временную таблицу, используйте следующий синтаксис:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

В большинстве случаев, временные таблицы создаются не вручную, а при использовании внешних данных для запроса, или при распределённом `(GLOBAL) IN`. Подробнее см. соответствующие разделы

Вместо временных можно использовать обычные таблицы с [ENGINE = Memory](../../sql-reference/statements/create.md).

## Распределенные DDL запросы (секция ON CLUSTER) {#raspredelennye-ddl-zaprosy-sektsiia-on-cluster}

Запросы `CREATE`, `DROP`, `ALTER`, `RENAME` поддерживают возможность распределенного выполнения на кластере.
Например, следующий запрос создает распределенную (Distributed) таблицу `all_hits` на каждом хосте в `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

Для корректного выполнения таких запросов необходимо на каждом хосте иметь одинаковое определение кластера (для упрощения синхронизации конфигов можете использовать подстановки из ZooKeeper). Также необходимо подключение к ZooKeeper серверам.
Локальная версия запроса в конечном итоге будет выполнена на каждом хосте кластера, даже если некоторые хосты в данный момент не доступны. Гарантируется упорядоченность выполнения запросов в рамках одного хоста.

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

Создаёт представление. Представления бывают двух видов - обычные и материализованные (MATERIALIZED).

Обычные представления не хранят никаких данных, а всего лишь производят чтение из другой таблицы. То есть, обычное представление - не более чем сохранённый запрос. При чтении из представления, этот сохранённый запрос, используется в качестве подзапроса в секции FROM.

Для примера, пусть вы создали представление:

``` sql
CREATE VIEW view AS SELECT ...
```

и написали запрос:

``` sql
SELECT a, b, c FROM view
```

Этот запрос полностью эквивалентен использованию подзапроса:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

Материализованные (MATERIALIZED) представления хранят данные, преобразованные соответствующим запросом SELECT.

При создании материализованного представления без использования `TO [db].[table]`, нужно обязательно указать ENGINE - движок таблицы для хранения данных.

При создании материализованного представления с испольованием `TO [db].[table]`, нельзя указывать `POPULATE`

Материализованное представление устроено следующим образом: при вставке данных в таблицу, указанную в SELECT-е, кусок вставляемых данных преобразуется этим запросом SELECT, и полученный результат вставляется в представление.

Если указано POPULATE, то при создании представления, в него будут вставлены имеющиеся данные таблицы, как если бы был сделан запрос `CREATE TABLE ... AS SELECT ...` . Иначе, представление будет содержать только данные, вставляемые в таблицу после создания представления. Не рекомендуется использовать POPULATE, так как вставляемые в таблицу данные во время создания представления, не попадут в него.

Запрос `SELECT` может содержать `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Следует иметь ввиду, что соответствующие преобразования будут выполняться независимо, на каждый блок вставляемых данных. Например, при наличии `GROUP BY`, данные будут агрегироваться при вставке, но только в рамках одной пачки вставляемых данных. Далее, данные не будут доагрегированы. Исключение - использование ENGINE, производящего агрегацию данных самостоятельно, например, `SummingMergeTree`.

Недоработано выполнение запросов `ALTER` над материализованными представлениями, поэтому они могут быть неудобными для использования. Если материализованное представление использует конструкцию `TO [db.]name`, то можно выполнить `DETACH` представления, `ALTER` для целевой таблицы и последующий `ATTACH` ранее отсоединенного (`DETACH`) представления.

Представления выглядят так же, как обычные таблицы. Например, они перечисляются в результате запроса `SHOW TABLES`.

Отсутствует отдельный запрос для удаления представлений. Чтобы удалить представление, следует использовать `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

Создаёт [внешний словарь](../../sql-reference/statements/create.md) с заданной [структурой](../../sql-reference/statements/create.md), [источником](../../sql-reference/statements/create.md), [способом размещения в памяти](../../sql-reference/statements/create.md) и [периодом обновления](../../sql-reference/statements/create.md).

Структура внешнего словаря состоит из атрибутов. Атрибуты словаря задаются как столбцы таблицы. Единственным обязательным свойством атрибута является его тип, все остальные свойства могут иметь значения по умолчанию.

В зависимости от [способа размещения словаря в памяти](../../sql-reference/statements/create.md), ключами словаря могут быть один и более атрибутов.

Смотрите [Внешние словари](../../sql-reference/statements/create.md).

## CREATE USER {#create-user-statement}

Создает [аккаунт пользователя](../../operations/access-rights.md#user-account-management).

### Синтаксис {#create-user-syntax}

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### Идентификация

Существует несколько способов идентификации пользователя:

- `IDENTIFIED WITH no_password`
- `IDENTIFIED WITH plaintext_password BY 'qwerty'`
- `IDENTIFIED WITH sha256_password BY 'qwerty'` or `IDENTIFIED BY 'password'`
- `IDENTIFIED WITH sha256_hash BY 'hash'`
- `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
- `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### Пользовательский хост

Пользовательский хост — это хост, с которого можно установить соединение с сервером ClickHouse. Хост задается в секции `HOST` следующими способами:

- `HOST IP 'ip_address_or_subnetwork'` — Пользователь может подключиться к серверу ClickHouse только с указанного IP-адреса или [подсети](https://ru.wikipedia.org/wiki/Подсеть). Примеры: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. При использовании в эксплуатации указывайте только элементы `HOST IP` (IP-адреса и маски подсети), так как использование `host` и `host_regexp` может привести к дополнительной задержке.
- `HOST ANY` — Пользователь может подключиться с любого хоста. Используется по умолчанию.
- `HOST LOCAL` — Пользователь может подключиться только локально.
- `HOST NAME 'fqdn'` — Хост задается через FQDN. Например, `HOST NAME 'mysite.com'`.
- `HOST NAME REGEXP 'regexp'` — Позволяет использовать регулярные выражения [pcre](http://www.pcre.org/), чтобы задать хосты. Например, `HOST NAME REGEXP '.*\.mysite\.com'`.
- `HOST LIKE 'template'` — Позволяет использовать оператор [LIKE](../functions/string-search-functions.md#function-like) для фильтрации хостов. Например, `HOST LIKE '%'` эквивалентен `HOST ANY`; `HOST LIKE '%.mysite.com'` разрешает подключение со всех хостов в домене `mysite.com`.

Также, чтобы задать хост, вы можете использовать `@` вместе с именем пользователя. Примеры:

- `CREATE USER mira@'127.0.0.1'` — Эквивалентно `HOST IP`.
- `CREATE USER mira@'localhost'` — Эквивалентно `HOST LOCAL`.
- `CREATE USER mira@'192.168.%.%'` — Эквивалентно `HOST LIKE`.

!!! info "Внимание"
    ClickHouse трактует конструкцию `user_name@'address'` как имя пользователя целиком. То есть технически вы можете создать несколько пользователей с одинаковыми `user_name`, но разными частями конструкции после `@`, но лучше так не делать.


### Примеры {#create-user-examples}


Создать аккаунт `mira`, защищенный паролем `qwerty`:

```sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

Пользователь `mira` должен запустить клиентское приложение на хосте, где запущен ClickHouse.

Создать аккаунт `john`, назначить на него роли, сделать данные роли ролями по умолчанию:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

Создать аккаунт `john` и установить ролями по умолчанию все его будущие роли:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Когда роль будет назначена аккаунту `john`, она автоматически станет ролью по умолчанию.

Создать аккаунт `john` и установить ролями по умолчанию все его будущие роли, кроме `role1` и `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```


## CREATE ROLE {#create-role-statement}

Создает [роль](../../operations/access-rights.md#role-management).

### Синтаксис {#create-role-syntax}

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Описание {#create-role-description}

Роль — это набор [привилегий](grant.md#grant-privileges). Пользователь, которому назначена роль, получает все привилегии этой роли.

Одному пользователю можно назначить несколько ролей. Пользователи могут применять назначенные роли в произвольных комбинациях с помощью выражения [SET ROLE](misc.md#set-role-statement). Конечный объем привилегий — это комбинация всех привилегий всех примененных ролей. Если у пользователя имеются привилегии, присвоенные его аккаунту напрямую, они также прибавляются к привилегиям, присвоенным через роли.

Роли по умолчанию применяются при входе пользователя в систему. Установить роли по умолчанию можно с помощью выражений [SET DEFAULT ROLE](misc.md#set-default-role-statement) или [ALTER USER](alter.md#alter-user-statement).

Для отзыва роли используется выражение [REVOKE](revoke.md).

Для удаления роли используется выражение [DROP ROLE](misc.md#drop-role-statement). Удаленная роль автоматически отзывается у всех пользователей, которым была назначена.

### Примеры {#create-role-examples}

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Такая последовательность запросов создаст роль `accountant`, у которой есть привилегия на чтение из базы данных `accounting`.

Назначить роль `accountant` аккаунту `mira`:

```sql
GRANT accountant TO mira;
```

После назначения роли пользователь может ее применить и выполнять разрешенные ей запросы. Например:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```

## CREATE ROW POLICY {#create-row-policy-statement}

Создает [фильтр для строк](../../operations/access-rights.md#row-policy-management), которые пользователь может прочесть из таблицы.

### Синтаксис {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### Секция AS {#create-row-policy-as}

С помощью данной секции можно создать политику разрешения или ограничения.

Политика разрешения предоставляет доступ к строкам. Разрешительные политики, которые применяются к одной таблице, объединяются с помощью логического оператора `OR`. Политики являются разрешительными по умолчанию.

Политика ограничения запрещает доступ к строкам. Ограничительные политики, которые применяются к одной таблице, объединяются логическим оператором `AND`.

Ограничительные политики применяются к строкам, прошедшим фильтр разрешительной политики. Если вы не зададите разрешительные политики, пользователь не сможет обращаться ни к каким строкам из таблицы.

#### Секция TO {#create-row-policy-to}

В секции `TO` вы можете перечислить как роли, так и пользователей. Например, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Ключевым словом `ALL` обозначаются все пользователи, включая текущего. Ключевые слова `ALL EXCEPT` позволяют исключить пользователей из списка всех пользователей. Например, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### Примеры

- `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
- `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`


## CREATE QUOTA {#create-quota-statement}

Создает [квоту](../../operations/access-rights.md#quotas-management), которая может быть присвоена пользователю или роли.

### Синтаксис {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### Пример {#create-quota-example}

Ограничить максимальное количество запросов для текущего пользователя до 123 запросов каждые 15 месяцев:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```


## CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Создает [профиль настроек](../../operations/access-rights.md#settings-profiles-management), который может быть присвоен пользователю или роли.

### Синтаксис {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

### Пример {#create-settings-profile-syntax}

Создать профиль настроек `max_memory_usage_profile`, который содержит значение и ограничения для настройки `max_memory_usage`. Присвоить профиль пользователю `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/create/) <!--hide-->
