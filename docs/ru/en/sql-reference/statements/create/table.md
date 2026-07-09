---
description: 'Документация по TABLE'
keywords: ['сжатие', 'кодек', 'схема', 'DDL']
sidebar_label: 'TABLE'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Создаёт новую таблицу. Этот запрос может иметь различные синтаксические формы в зависимости от конкретного сценария использования.

По умолчанию таблицы создаются только на текущем сервере. Распределённые DDL-запросы реализованы в виде предложения `ON CLUSTER`, которое [описано отдельно](../../../sql-reference/distributed-ddl.md).

<div id="syntax-forms">
  ## Формы синтаксиса
</div>

<div id="with-explicit-schema">
  ### С явной схемой
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

Создаёт таблицу с именем `table_name` в базе данных `db` или в текущей базе данных, если `db` не задана, со структурой, указанной в скобках, и с движком `engine`.
Структура таблицы представляет собой список описаний столбцов, вторичных индексов, проекций и ограничений. Если движок поддерживает [первичный ключ](#primary-key), он будет указан как параметр для движка таблицы.

В простейшем случае описание столбца имеет вид `name type`. Пример: `RegionID UInt32`.

Для значений по умолчанию также можно задавать выражения (см. ниже).

При необходимости можно указать первичный ключ, состоящий из одного или нескольких ключевых выражений.

Для столбцов и таблицы можно добавлять комментарии.

<div id="with-a-schema-similar-to-other-table">
  ### Со схемой существующей таблицы
</div>

ClickHouse позволяет скопировать схему и данные существующей таблицы.

Чтобы скопировать схему существующей таблицы:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

Это создаёт таблицу с такой же структурой, как у другой таблицы.

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### Со схемой и данными существующей таблицы
</div>

Чтобы скопировать схему и данные существующей таблицы:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

Это создаёт таблицу с той же схемой и данными, что и существующая таблица. После создания новой таблицы к ней присоединяются все партиции из `db.table`. Иными словами, при создании данные из `db.table` клонируются в `db2.table_clone`. Этот запрос эквивалентен следующему:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

Для обеих возможностей можно указать для таблицы другой движок. Если движок не указан, будет использоваться тот же движок, что и для исходной таблицы (`db.table`).

<div id="from-a-table-function">
  ### Из табличной функции
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Создаёт таблицу с тем же результатом, что и указанная [табличная функция](/ru/sql-reference/table-functions). Созданная таблица также будет работать так же, как соответствующая указанная табличная функция.

<div id="from-select-query">
  ### Из SELECT-запроса
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

Создаёт таблицу со структурой, аналогичной результату запроса `SELECT`, с движком `engine` и заполняет её данными из `SELECT`. Также можно явно указать описание столбцов.

Если таблица уже существует и указан `IF NOT EXISTS`, запрос не выполнит никаких действий.

После предложения `ENGINE` в запросе могут идти и другие предложения. Подробную документацию о создании таблиц см. в описаниях [движков таблиц](/ru/engines/table-engines).

**Пример**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## Модификаторы `NULL` или `NOT NULL`
</div>

Модификаторы `NULL` и `NOT NULL` после типа данных в определении столбца разрешают или запрещают делать его [Nullable](/ru/sql-reference/data-types/nullable).

Если тип не `Nullable` и указан `NULL`, он будет считаться `Nullable`; если указан `NOT NULL`, то нет. Например, `INT NULL` — это то же самое, что `Nullable(INT)`. Если тип — `Nullable` и указаны модификаторы `NULL` или `NOT NULL`, будет сгенерировано исключение.

См. также настройку [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable).

<div id="default_values">
  ## Значения по умолчанию
</div>

Описание столбца может включать выражение значения по умолчанию в форме `DEFAULT expr`, `MATERIALIZED expr` или `ALIAS expr`. Пример: `URLDomain String DEFAULT domain(URL)`.

Выражение `expr` необязательно. Если оно опущено, тип столбца должен быть указан явно, а значением по умолчанию будет `0` для числовых столбцов, `''` (пустая строка) для столбцов String, `[]` (пустой массив) для столбцов типа Array, `1970-01-01` для столбцов Date или `NULL` для столбцов с типом Nullable.

Тип столбца для значения по умолчанию можно не указывать — тогда он выводится из типа `expr`. Например, тип столбца `EventDate DEFAULT toDate(EventTime)` будет Date.

Если указаны и тип данных, и выражение значения по умолчанию, добавляется неявная функция приведения типов, которая преобразует выражение к указанному типу. Пример: `Hits UInt32 DEFAULT 0` внутри представляется как `Hits UInt32 DEFAULT toUInt32(0)`.

Выражение значения по умолчанию `expr` может ссылаться на произвольные столбцы таблицы и константы. ClickHouse проверяет, что изменения структуры таблицы не приводят к появлению циклов при вычислении выражения. Для INSERT также проверяется, что выражения разрешимы, то есть что переданы все столбцы, на основе которых их можно вычислить.

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

Обычное значение по умолчанию. Если значение такого столбца не указано в запросе INSERT, оно вычисляется по `expr`.

Пример:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

Материализованное выражение. Значения таких столбцов автоматически вычисляются на основе указанного материализованного выражения при вставке строк. Явно задавать значения при `INSERT` нельзя.

Кроме того, столбцы этого типа со значением по умолчанию не включаются в результат `SELECT *`. Это позволяет сохранить инвариант: результат `SELECT *` всегда можно вставить обратно в таблицу с помощью `INSERT`. Это поведение можно отключить с помощью настройки `asterisk_include_materialized_columns`.

Пример:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

Эфемерный столбец. Столбцы этого типа не хранятся в таблице, и выполнить `SELECT` по ним нельзя. Единственное назначение эфемерных столбцов — использовать их для построения выражений значений по умолчанию для других столбцов.

При вставке без явного указания столбцов столбцы этого типа будут пропущены. Это нужно, чтобы сохранить инвариант: результат `SELECT *` всегда можно вставить обратно в таблицу с помощью `INSERT`.

Пример:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

Вычисляемые столбцы (синоним). Столбцы этого типа не хранятся в таблице, и в них нельзя вставлять значения с помощью `INSERT`.

Когда запросы SELECT явно ссылаются на столбцы этого типа, значение вычисляется из `expr` во время выполнения запроса. По умолчанию `SELECT *` исключает столбцы ALIAS. Это поведение можно отключить с помощью параметра `asterisk_include_alias_columns`.

При использовании запроса ALTER для добавления новых столбцов старые данные для этих столбцов не записываются. Вместо этого при чтении старых данных, у которых нет значений для новых столбцов, выражения по умолчанию вычисляются на лету. Однако если для вычисления выражений требуются другие столбцы, не указанные в запросе, эти столбцы также будут прочитаны, но только для тех блоков данных, где это необходимо.

Если вы добавите в таблицу новый столбец, а затем измените его выражение по умолчанию, значения, используемые для старых данных, изменятся (для данных, значения которых не были сохранены на диске). Обратите внимание, что при выполнении фоновых слияний данные для столбцов, отсутствующих в одной из объединяемых частей, записываются в слитую часть.

Невозможно задать значения по умолчанию для элементов во вложенных структурах данных.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## Первичный ключ
</div>

При создании таблицы вы можете определить [первичный ключ](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries). Первичный ключ можно задать двумя способами:

* В списке столбцов

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* За пределами списка столбцов

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
Нельзя использовать оба способа в одном запросе.
:::

<div id="constraints">
  ## Ограничения
</div>

Помимо описаний столбцов можно задавать ограничения:

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` может быть любым булевым выражением. Если для таблицы определены ограничения, каждое из них будет проверяться для каждой строки в запросе `INSERT`. Если какое-либо ограничение не соблюдается, сервер сгенерирует исключение с именем ограничения и проверяемым выражением.

Добавление большого количества ограничений может негативно сказаться на производительности больших запросов `INSERT`.

Существующие ограничения для всех таблиц можно просмотреть в таблице [`system.constraints`](/ru/operations/system-tables/constraints).

<div id="assume">
  ### ASSUME
</div>

Предложение `ASSUME` используется для задания `CONSTRAINT` для таблицы, которое считается истинным. Затем это ограничение может использоваться оптимизатором для повышения производительности SQL-запросов.

Рассмотрим пример, в котором `ASSUME CONSTRAINT` используется при создании таблицы `users_a`:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

Здесь `ASSUME CONSTRAINT` используется, чтобы указать, что значение функции `length(name)` всегда равно значению столбца `name_len`. Это означает, что всякий раз, когда в запросе вызывается `length(name)`, ClickHouse может заменить её на `name_len`, что должно работать быстрее, поскольку позволяет избежать вызова функции `length()`.

Затем, при выполнении запроса `SELECT name FROM users_a WHERE length(name) < 5;`, ClickHouse может оптимизировать его до `SELECT name FROM users_a WHERE name_len < 5`; благодаря `ASSUME CONSTRAINT`. Это может ускорить выполнение запроса, поскольку не нужно вычислять длину `name` для каждой строки.

`ASSUME CONSTRAINT` **не накладывает ограничение**, а лишь сообщает оптимизатору, что ограничение выполняется. Если на самом деле это не так, результаты запросов могут быть неверными. Поэтому использовать `ASSUME CONSTRAINT` следует только в том случае, если вы уверены, что ограничение действительно выполняется.

<div id="ttl-expression">
  ## Выражение TTL
</div>

Задаёт срок хранения значений. Может быть указано только для таблиц семейства MergeTree. Подробное описание см. в разделе [TTL для столбцов и таблиц](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

<div id="column_compression_codec">
  ## Кодеки сжатия столбцов
</div>

По умолчанию ClickHouse использует сжатие `lz4` в самоуправляемом варианте и `zstd` в ClickHouse Cloud.

Для семейства движков `MergeTree` метод сжатия по умолчанию можно изменить в разделе [compression](/ru/operations/server-configuration-parameters/settings#compression) конфигурации сервера.

Вы также можете задать метод сжатия для каждого отдельного столбца в запросе `CREATE TABLE`.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

Кодек `Default` можно указать, чтобы использовать сжатие по умолчанию, которое во время выполнения может зависеть от различных настроек (и свойств данных).
Пример: `value UInt64 CODEC(Default)` — то же самое, что не указывать кодек.

Также можно удалить текущий CODEC из столбца и использовать сжатие по умолчанию из config.xml:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

Кодеки можно комбинировать в цепочку, например, `CODEC(Delta, Default)`.

:::tip
Файлы базы данных ClickHouse нельзя распаковать с помощью внешних утилит, таких как `lz4`. Вместо этого используйте специальную утилиту [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor).
:::

Сжатие поддерживается для следующих движков таблиц:

* Семейство [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md). Поддерживает кодеки сжатия для столбцов и выбор метода сжатия по умолчанию через настройки [compression](/ru/operations/server-configuration-parameters/settings#compression).
* Семейство [Log](../../../engines/table-engines/log-family/index.md). По умолчанию использует метод сжатия `lz4` и поддерживает кодеки сжатия для столбцов.
* [Set](../../../engines/table-engines/special/set.md). Поддерживается только сжатие по умолчанию.
* [Join](../../../engines/table-engines/special/join.md). Поддерживается только сжатие по умолчанию.

ClickHouse поддерживает кодеки общего назначения и специализированные кодеки.

<div id="general-purpose-codecs">
  ### Кодеки общего назначения
</div>

<div id="none">
  #### NONE
</div>

`NONE` — без сжатия.

<div id="lz4">
  #### LZ4
</div>

`LZ4` — алгоритм [сжатия данных](https://github.com/lz4/lz4) без потерь, используемый по умолчанию. Обеспечивает быстрое сжатие LZ4.

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — алгоритм LZ4 HC (высокая степень сжатия) с настраиваемым уровнем. Уровень по умолчанию: 9. При значении `level <= 0` используется уровень по умолчанию. Возможные уровни: [1, 12]. Рекомендуемый диапазон уровней: [4, 9].

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — [алгоритм сжатия ZSTD](https://en.wikipedia.org/wiki/Zstandard) с настраиваемым `level`. Возможные уровни: [1, 22]. Уровень по умолчанию: 1.

Высокие уровни сжатия полезны для асимметричных сценариев, например когда данные сжимаются один раз, а распаковываются многократно. Более высокие уровни обеспечивают лучшее сжатие и более высокую загрузку CPU.

<div id="zstd_qat">
  #### Устарело: ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### Устарело: DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### Специализированные кодеки
</div>

Эти кодеки предназначены для повышения эффективности сжатия за счёт использования особенностей данных. Некоторые из них сами по себе данные не сжимают, а лишь предварительно обрабатывают их, чтобы на втором этапе сжатия с применением кодека общего назначения можно было добиться более высокой степени сжатия.

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — подход к сжатию, при котором исходные значения заменяются разностью двух соседних значений, за исключением первого, которое остается неизменным. `delta_bytes` — максимальный размер исходных значений; значение по умолчанию — `sizeof(type)`. Указывать `delta_bytes` в качестве аргумента не рекомендуется, и в одной из будущих версий поддержка будет удалена. Delta — это кодек подготовки данных, то есть его нельзя использовать отдельно.

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — вычисляет дельту дельт и записывает её в компактной бинарной форме. `bytes_size` имеет значение, аналогичное `delta_bytes` в кодеке [Delta](#delta). Использование `bytes_size` в качестве аргумента устарело, и его поддержка будет удалена в одном из будущих релизов. Наилучшие показатели сжатия достигаются для монотонных последовательностей с постоянным шагом, таких как данные временных рядов. Может использоваться с любым числовым типом. Реализует алгоритм, используемый в Gorilla TSDB, расширяя его для поддержки 64-битных типов. Использует 1 дополнительный бит для 32-битных дельт: 5-битные префиксы вместо 4-битных. Дополнительные сведения см. в разделе Compressing Time Stamps статьи [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). DoubleDelta — это кодек подготовки данных, то есть его нельзя использовать отдельно.

<div id="gcd">
  #### GCD
</div>

`GCD()` - - Вычисляет наибольший общий делитель (GCD) значений в столбце, а затем делит каждое значение на GCD. Может использоваться со столбцами целых, десятичных чисел и даты/времени. Этот кодек хорошо подходит для столбцов со значениями, которые изменяются (увеличиваются или уменьшаются) с шагом, кратным GCD, например: 24, 28, 16, 24, 8, 24 (GCD = 4). GCD — кодек подготовки данных, то есть его нельзя использовать отдельно.

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — вычисляет XOR между текущим и предыдущим значением с плавающей запятой и записывает результат в компактной двоичной форме. Чем меньше разность между последовательными значениями, то есть чем медленнее изменяются значения временного ряда, тем лучше степень сжатия. Реализует алгоритм, используемый в Gorilla TSDB, с расширением поддержки 64-битных типов. Возможные значения `bytes_size`: 1, 2, 4, 8; значение по умолчанию — `sizeof(type)`, если оно равно 1, 2, 4 или 8. Во всех остальных случаях используется 1. Дополнительную информацию см. в разделе 4.1 статьи [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — адаптивное сжатие без потерь для данных с плавающей запятой. Поддерживает `Float32` и `Float64`. Подробнее см. [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334).

Кодек принимает необязательный аргумент варианта:

* `ALP()` или `ALP(AUTO)` (по умолчанию) — использует STD и при необходимости переключается на RD на основе оценённого размера после сжатия.
* `ALP(STD)` — стандартный вариант ALP. Представляет каждое значение как точное масштабированное целое число с использованием десятичных степеней, а затем сжимает полученные целые числа с помощью Frame-of-Reference и упаковки битов. Значения, которые так представить нельзя, сохраняются в исходном виде как исключения. Лучше всего подходит для чисел, представленных в десятичной форме (например, измерений, цен).
* `ALP(RD)` — вариант Real Doubles. Повторно интерпретирует битовое представление каждого значения и разделяет его на старшую часть (знак + экспонента + старшие биты мантиссы) и младшую часть. Старшие части кодируются с помощью словаря (до 8 значений), младшие упаковываются по битам. Лучше всего подходит для случаев, когда у многих значений совпадают старшие биты.

:::note
Этот кодек является экспериментальным, и для его использования требуется `SET allow_experimental_codecs = 1`.
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` — многократно предсказывает следующее значение с плавающей запятой в последовательности, выбирая лучший из двух предикторов, затем применяет XOR к фактическому и предсказанному значениям и сжимает результат по ведущим нулям. Как и Gorilla, этот кодек эффективен при хранении последовательности значений с плавающей запятой, которые изменяются медленно. Для 64-битных значений (double) FPC работает быстрее, чем Gorilla; для 32-битных значений результат может отличаться. Возможные значения `level`: 1–28, значение по умолчанию — 12. Возможные значения `float_size`: 4, 8; значение по умолчанию — `sizeof(type)`, если тип — Float. Во всех остальных случаях это 4. Подробное описание алгоритма см. в работе [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

<div id="t64">
  #### T64
</div>

`T64` — метод сжатия, при котором отсекаются неиспользуемые старшие биты значений в целочисленных типах данных (включая `Enum`, `Date` и `DateTime`). На каждом шаге алгоритма кодек берет блок из 64 значений, помещает их в битовую матрицу 64x64, транспонирует ее, отсекает неиспользуемые биты значений и возвращает оставшуюся часть в виде последовательности. Неиспользуемые биты — это биты, которые не различаются у максимального и минимального значений во всей части данных, для которой применяется сжатие.

Кодеки `DoubleDelta` и `Gorilla` используются в Gorilla TSDB как компоненты алгоритма сжатия. Подход Gorilla эффективен в сценариях, где есть последовательность медленно меняющихся значений с соответствующими временными метками. Временные метки эффективно сжимаются кодеком `DoubleDelta`, а значения — кодеком `Gorilla`. Например, чтобы таблица хранилась эффективно, ее можно создать в следующей конфигурации:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### Кодеки шифрования
</div>

Эти кодеки на самом деле не сжимают данные, а шифруют их на диске. Они доступны только при указании ключа шифрования в настройках [encryption](/ru/operations/server-configuration-parameters/settings#encryption). Обратите внимание, что шифрование имеет смысл только в конце конвейеров кодеков, поскольку зашифрованные данные обычно уже нельзя сколько-нибудь эффективно сжать.

Кодеки шифрования:

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — шифрует данные с помощью AES-128 в режиме GCM-SIV, описанном в [RFC 8452](https://tools.ietf.org/html/rfc8452).

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — шифрует данные с помощью AES-256 в режиме GCM-SIV.

Эти кодеки используют фиксированный nonce, поэтому шифрование является детерминированным. Это делает их совместимыми с движками, поддерживающими дедупликацию, такими как [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md), но у этого есть недостаток: если один и тот же блок данных зашифровать дважды, результирующий шифртекст будет в точности одинаковым, поэтому злоумышленник, имеющий доступ к чтению диска, сможет увидеть эту эквивалентность (хотя и только эквивалентность, без доступа к содержимому).

:::note
Большинство движков, включая семейство &quot;*MergeTree&quot;, создают на диске индексные файлы без применения кодеков. Это означает, что открытый текст окажется на диске, если для зашифрованного столбца создан индекс.
:::

:::note
Если вы выполняете запрос SELECT с указанием конкретного значения в зашифрованном столбце (например, в предложении WHERE), это значение может появиться в [system.query&#95;log](../../../operations/system-tables/query_log.md). Возможно, вы захотите отключить журналирование.
:::

**Пример**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
Если нужно применить сжатие, это необходимо указать явно. В противном случае к данным будет применяться только шифрование.
:::

**Пример**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## Временные таблицы
</div>

:::note
Обратите внимание: временные таблицы не реплицируются. Поэтому нет гарантии, что данные, вставленные во временную таблицу, будут доступны на других репликах. Временные таблицы в основном полезны для выполнения запросов к небольшим внешним датасетам или JOIN с ними в рамках одного сеанса.
:::

ClickHouse поддерживает временные таблицы со следующими характеристиками:

* Временные таблицы исчезают после завершения сеанса, в том числе при потере соединения.
* Временная таблица использует движок таблицы Memory, если движок не указан, и может использовать любой движок таблицы, кроме движков Replicated и `KeeperMap`.
* Для временной таблицы нельзя указать БД. Она создаётся вне баз данных.
* Невозможно создать временную таблицу с помощью распределённого DDL-запроса на всех серверах кластера (с использованием `ON CLUSTER`): такая таблица существует только в текущем сеансе.
* Если временная таблица имеет то же имя, что и другая таблица, и в запросе указано имя таблицы без БД, будет использована временная таблица.
* При распределённой обработке запросов временные таблицы с движком Memory, используемые в запросе, передаются на удалённые серверы.

Чтобы создать временную таблицу, используйте следующий синтаксис:

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

В большинстве случаев временные таблицы создаются не вручную, а при использовании внешних данных в запросе или распределённого `(GLOBAL) IN`. Дополнительные сведения см. в соответствующих разделах.

Вместо временных таблиц можно использовать таблицы с [ENGINE = Memory](../../../engines/table-engines/special/memory.md).

<div id="replace-table">
  ## REPLACE TABLE
</div>

Оператор `REPLACE` позволяет [атомарно](/ru/concepts/glossary#atomicity) обновлять таблицу.

:::note
Этот оператор поддерживается для движков баз данных [`Atomic`](../../../engines/database-engines/atomic.md) и [`Replicated`](../../../engines/database-engines/replicated.md),
которые являются движками баз данных по умолчанию для ClickHouse и ClickHouse Cloud соответственно.
:::

Как правило, если вам нужно удалить часть данных из таблицы,
можно создать новую таблицу и заполнить её с помощью оператора `SELECT`, который не выбирает нежелательные данные,
затем удалить старую таблицу и переименовать новую.
Этот подход показан в примере ниже:

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

Вместо описанного выше подхода можно также использовать `REPLACE` (если вы используете движки баз данных по умолчанию), чтобы получить тот же результат:

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### Синтаксис
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
Для этого оператора также подходят все синтаксические формы оператора `CREATE`. Вызов `REPLACE` для несуществующей таблицы приведет к ошибке.
:::

<div id="examples">
  ### Примеры:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="Локально" default>
    Рассмотрим следующую таблицу:

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    Мы можем использовать оператор `REPLACE`, чтобы удалить все данные:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    Или мы можем использовать оператор `REPLACE`, чтобы изменить структуру таблицы:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    Рассмотрим следующую таблицу в ClickHouse Cloud:

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    Мы можем использовать оператор `REPLACE`, чтобы удалить все данные:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    Или мы можем использовать оператор `REPLACE`, чтобы изменить структуру таблицы:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## Предложение COMMENT
</div>

При создании таблицы можно добавить к ней комментарий.

**Синтаксис**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
Предложение `COMMENT` должно быть указано **после** всех предложений, относящихся к хранилищу, таких как `PARTITION BY`, `ORDER BY` и `SETTINGS`, специфичных для хранилища.

После предложения `COMMENT` будут разобраны только `SETTINGS`, относящиеся к запросу (например, `max_threads` и т. д.), а не настройки, связанные с хранилищем.

Это означает, что правильный порядок предложений такой:

* `ENGINE`
* предложения хранилища
* `COMMENT`
* настройки запроса (если есть)
  :::

**Пример**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Оптимизация ClickHouse с помощью схем и кодеков](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Блог: [Работа с данными временных рядов в ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)