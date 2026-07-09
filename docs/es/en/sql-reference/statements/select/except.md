---
description: 'Documentación de la cláusula EXCEPT, que devuelve únicamente las filas obtenidas por la primera consulta y no por la segunda.'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except
title: 'Cláusula EXCEPT'
keywords: ['EXCEPT', 'cláusula']
doc_type: 'referencia'
---

> La cláusula `EXCEPT` devuelve únicamente las filas obtenidas por la primera consulta y no por la segunda.

* Ambas consultas deben tener el mismo número de columnas, en el mismo orden y con el mismo tipo de datos.
* El resultado de `EXCEPT` puede contener filas duplicadas. Use `EXCEPT DISTINCT` si no lo desea.
* Varias sentencias `EXCEPT` se ejecutan de izquierda a derecha si no se especifican paréntesis.
* El operador `EXCEPT` tiene la misma prioridad que la cláusula `UNION` y menor prioridad que la cláusula `INTERSECT`.

<div id="syntax">
  ## Sintaxis
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

La condición puede ser cualquier expresión, según sus necesidades.

Además, `EXCEPT()` puede usarse para excluir columnas de un resultado dentro de la misma tabla, como ocurre en BigQuery (Google Cloud), con la siguiente sintaxis:

```sql
SELECT column1 [, column2 ] EXCEPT (column3 [, column4]) 
FROM table1 
[WHERE condition]
```

<div id="examples">
  ## Ejemplos
</div>

Los ejemplos de esta sección muestran el uso de la cláusula `EXCEPT`.

<div id="filtering-numbers-using-the-except-clause">
  ### Filtrar números con la cláusula `EXCEPT`
</div>

A continuación se muestra un ejemplo sencillo que devuelve los números del 1 al 10 que *no* están incluidos entre los números del 3 al 8:

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
  ### Excluir columnas específicas con `EXCEPT()`
</div>

`EXCEPT()` puede usarse para excluir rápidamente columnas de un resultado. Por ejemplo, si queremos seleccionar todas las columnas de una tabla excepto unas pocas columnas concretas, como se muestra en el ejemplo siguiente:

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
  ### Uso de `EXCEPT` e `INTERSECT` con datos de criptomonedas
</div>

A menudo, `EXCEPT` e `INTERSECT` pueden usarse indistintamente con distinta lógica booleana, y ambos resultan útiles si tiene dos tablas que comparten una columna común (o varias columnas).
Por ejemplo, supongamos que tenemos unos millones de filas de datos históricos de criptomonedas que contienen precios de negociación y volumen:

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

Ahora supongamos que tenemos una tabla llamada `holdings` que contiene una lista de criptomonedas que tenemos, junto con la cantidad de unidades:

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

Podemos usar `EXCEPT` para responder a una pregunta como **&quot;¿Qué monedas de las que poseemos nunca han cotizado por debajo de $10?&quot;**:

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

Esto significa que, de las cuatro criptomonedas que tenemos, solo Bitcoin nunca ha caído por debajo de los 10 $ (según los datos limitados que tenemos en este ejemplo).

<div id="using-except-distinct">
  ### Uso de `EXCEPT DISTINCT`
</div>

Observe que en la consulta anterior había varias tenencias de Bitcoin en el resultado. Puede añadir `DISTINCT` a `EXCEPT` para eliminar las filas duplicadas del resultado:

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

**Véase también**

* [UNION](/es/sql-reference/statements/select/union)
* [INTERSECT](/es/sql-reference/statements/select/intersect)