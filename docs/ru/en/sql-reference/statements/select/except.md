---
description: 'Документация по оператору EXCEPT, который возвращает только те строки из первого запроса, которых нет во втором.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except
title: 'Оператор EXCEPT'
keywords: ['EXCEPT', 'оператор']
doc_type: 'reference'
---

> Оператор `EXCEPT` возвращает только те строки из первого запроса, которых нет во втором.

* Оба запроса должны иметь одинаковое количество столбцов в одном и том же порядке и с одинаковыми типами данных.
* Результат `EXCEPT` может содержать повторяющиеся строки. Используйте `EXCEPT DISTINCT`, если это нежелательно.
* Если скобки не указаны, несколько операторов `EXCEPT` выполняются слева направо.
* Оператор `EXCEPT` имеет тот же приоритет, что и оператор `UNION`, и более низкий приоритет, чем оператор `INTERSECT`.

<div id="syntax">
  ## Синтаксис
</div>

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]
```

Условие может быть любым выражением в соответствии с вашими требованиями.

Кроме того, `EXCEPT()` можно использовать для исключения столбцов из результата в той же таблице, как в BigQuery (Google Cloud), используя следующий синтаксис:

```sql
SELECT column1 [, column2 ] EXCEPT (column3 [, column4]) 
FROM table1 
[WHERE condition]
```

<div id="examples">
  ## Примеры
</div>

Примеры в этом разделе демонстрируют использование оператора `EXCEPT`.

<div id="filtering-numbers-using-the-except-clause">
  ### Фильтрация чисел с помощью оператора `EXCEPT`
</div>

Вот простой пример, который возвращает числа от 1 до 10, *не* входящие в диапазон от 3 до 8:

```sql title="Query"
SELECT number
FROM numbers(1, 10)
EXCEPT
SELECT number
FROM numbers(3, 6)
```

```response title="Response"
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

<div id="excluding-specific-columns-using-except">
  ### Исключение отдельных столбцов с помощью `EXCEPT()`
</div>

`EXCEPT()` позволяет быстро исключить столбцы из результата. Например, если нужно выбрать из таблицы все столбцы, кроме нескольких, как показано в примере ниже:

```sql title="Query"
SHOW COLUMNS IN system.settings

SELECT * EXCEPT (default, alias_for, readonly, description)
FROM system.settings
LIMIT 5
```

```response title="Response"
    ┌─field───────┬─type─────────────────────────────────────────────────────────────────────┬─null─┬─key─┬─default─┬─extra─┐
 1. │ alias_for   │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 2. │ changed     │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
 3. │ default     │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 4. │ description │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 5. │ is_obsolete │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
 6. │ max         │ Nullable(String)                                                         │ YES  │     │ ᴺᵁᴸᴸ    │       │
 7. │ min         │ Nullable(String)                                                         │ YES  │     │ ᴺᵁᴸᴸ    │       │
 8. │ name        │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 9. │ readonly    │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
10. │ tier        │ Enum8('Production' = 0, 'Obsolete' = 4, 'Experimental' = 8, 'Beta' = 12) │ NO   │     │ ᴺᵁᴸᴸ    │       │
11. │ type        │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
12. │ value       │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
    └─────────────┴──────────────────────────────────────────────────────────────────────────┴──────┴─────┴─────────┴───────┘

   ┌─name────────────────────┬─value──────┬─changed─┬─min──┬─max──┬─type────┬─is_obsolete─┬─tier───────┐
1. │ dialect                 │ clickhouse │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Dialect │           0 │ Production │
2. │ min_compress_block_size │ 65536      │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
3. │ max_compress_block_size │ 1048576    │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
4. │ max_block_size          │ 65409      │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
5. │ max_insert_block_size   │ 1048449    │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
   └─────────────────────────┴────────────┴─────────┴──────┴──────┴─────────┴─────────────┴────────────┘
```

<div id="using-except-and-intersect-with-cryptocurrency-data">
  ### Использование `EXCEPT` и `INTERSECT` с данными о криптовалютах
</div>

`EXCEPT` и `INTERSECT` часто можно использовать как взаимозаменяемые операторы при разной булевой логике; оба они полезны, если у вас есть две таблицы с общим столбцом (или несколькими столбцами).
Например, предположим, что у нас есть несколько миллионов строк исторических данных о криптовалютах, содержащих цены сделок и объёмы торгов:

```sql title="Query"
CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT *
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

SELECT * FROM crypto_prices
WHERE crypto_name = 'Bitcoin'
ORDER BY trade_date DESC
LIMIT 10;
```

```response title="Response"
┌─trade_date─┬─crypto_name─┬──────volume─┬────price─┬───market_cap─┬──change_1_day─┐
│ 2020-11-02 │ Bitcoin     │ 30771456000 │ 13550.49 │ 251119860000 │  -0.013585099 │
│ 2020-11-01 │ Bitcoin     │ 24453857000 │ 13737.11 │ 254569760000 │ -0.0031840964 │
│ 2020-10-31 │ Bitcoin     │ 30306464000 │ 13780.99 │ 255372070000 │   0.017308505 │
│ 2020-10-30 │ Bitcoin     │ 30581486000 │ 13546.52 │ 251018150000 │   0.008084608 │
│ 2020-10-29 │ Bitcoin     │ 56499500000 │ 13437.88 │ 248995320000 │   0.012552661 │
│ 2020-10-28 │ Bitcoin     │ 35867320000 │ 13271.29 │ 245899820000 │   -0.02804481 │
│ 2020-10-27 │ Bitcoin     │ 33749879000 │ 13654.22 │ 252985950000 │    0.04427984 │
│ 2020-10-26 │ Bitcoin     │ 29461459000 │ 13075.25 │ 242251000000 │  0.0033826586 │
│ 2020-10-25 │ Bitcoin     │ 24406921000 │ 13031.17 │ 241425220000 │ -0.0058658565 │
│ 2020-10-24 │ Bitcoin     │ 24542319000 │ 13108.06 │ 242839880000 │   0.013650347 │
└────────────┴─────────────┴─────────────┴──────────┴──────────────┴───────────────┘
```

Теперь предположим, что у нас есть таблица `holdings`, в которой перечислены принадлежащие нам криптовалюты и указано количество монет:

```sql title="Query"
CREATE TABLE holdings
(
    crypto_name String,
    quantity UInt64
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name);

INSERT INTO holdings VALUES
   ('Bitcoin', 1000),
   ('Bitcoin', 200),
   ('Ethereum', 250),
   ('Ethereum', 5000),
   ('DOGEFI', 10),
   ('Bitcoin Diamond', 5000);
```

Мы можем использовать `EXCEPT`, чтобы ответить на вопрос **&quot;Какие из принадлежащих нам монет никогда не торговались ниже $10?&quot;**:

```sql title="Query"
SELECT crypto_name FROM holdings
EXCEPT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
└─────────────┘
```

Это означает, что из четырёх криптовалют, которыми мы владеем, только Bitcoin ни разу не опускался ниже $10 (судя по ограниченным данным, которые есть у нас в этом примере).

<div id="using-except-distinct">
  ### Использование `EXCEPT DISTINCT`
</div>

Обратите внимание, что в результате предыдущего запроса у нас было несколько позиций Bitcoin. Вы можете добавить `DISTINCT` к `EXCEPT`, чтобы удалить из результата повторяющиеся строки:

```sql title="Query"
SELECT crypto_name FROM holdings
EXCEPT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
└─────────────┘
```

**См. также**

* [UNION](/ru/sql-reference/statements/select/union)
* [INTERSECT](/ru/sql-reference/statements/select/intersect)