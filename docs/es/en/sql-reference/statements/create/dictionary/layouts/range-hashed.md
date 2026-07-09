---
slug: /sql-reference/statements/create/dictionary/layouts/range-hashed
title: 'tipos de diseño de diccionario range_hashed'
sidebar_label: 'range_hashed'
sidebar_position: 5
description: 'Almacena un diccionario en memoria mediante una tabla hash con intervalos de fecha/hora ordenados.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="range_hashed">
  ## range_hashed
</div>

El diccionario se almacena en memoria en forma de una tabla hash con una array ordenada de rangos y sus valores correspondientes.

Este método de almacenamiento funciona igual que hashed y permite usar rangos de fecha/hora (de cualquier tipo numérico), además de la clave.

Ejemplo: La tabla contiene descuentos para cada anunciante en el formato:

```text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

Para usar rangos de fechas, defina los elementos `range_min` y `range_max` en la [estructura](../attributes.md#composite-key). Estos elementos deben contener `name` y `type` (si no se especifica `type`, se usará el tipo predeterminado: Date). `type` puede ser cualquier tipo numérico (Date / DateTime / UInt64 / Int32 / otros).

:::note
Los valores de `range_min` y `range_max` deben ajustarse al tipo `Int64`.
:::

Ejemplo:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY discounts_dict (
        advertiser_id UInt64,
        discount_start_date Date,
        discount_end_date Date,
        amount Float64
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'discounts'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
    RANGE(MIN discount_start_date MAX discount_end_date)
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <layout>
        <range_hashed>
            <!-- Estrategia para rangos superpuestos (min/max). Valor predeterminado: min (devuelve un rango coincidente con el valor min(range_min -> range_max)) -->
            <range_lookup_strategy>min</range_lookup_strategy>
        </range_hashed>
    </layout>
    <structure>
        <id>
            <name>advertiser_id</name>
        </id>
        <range_min>
            <name>discount_start_date</name>
            <type>Date</type>
        </range_min>
        <range_max>
            <name>discount_end_date</name>
            <type>Date</type>
        </range_max>
        ...
    ```
  </TabItem>
</Tabs>

<br />

Para trabajar con estos diccionarios, debe pasar un argumento adicional a la función `dictGet`, para el que se selecciona un rango:

```sql
dictGet('dict_name', 'attr_name', id, date)
```

Ejemplo de consulta:

```sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

Esta función devuelve el valor de los `id` especificados y el intervalo de fechas que incluye la fecha proporcionada.

Detalles del algoritmo:

* Si no se encuentra el `id` o no se encuentra ningún rango para el `id`, devuelve el valor predeterminado del tipo del atributo.
* Si hay rangos superpuestos y `range_lookup_strategy=min`, devuelve un rango coincidente con el `range_min` mínimo; si se encuentran varios rangos, devuelve uno con el `range_max` mínimo; si aun así se encuentran varios rangos (varios rangos tenían el mismo `range_min` y `range_max`), devuelve uno aleatorio de entre ellos.
* Si hay rangos superpuestos y `range_lookup_strategy=max`, devuelve un rango coincidente con el `range_min` máximo; si se encuentran varios rangos, devuelve uno con el `range_max` máximo; si aun así se encuentran varios rangos (varios rangos tenían el mismo `range_min` y `range_max`), devuelve uno aleatorio de entre ellos.
* Si `range_max` es `NULL`, el rango es abierto. `NULL` se trata como el valor máximo posible. Para `range_min`, se pueden usar `1970-01-01` o `0` (-MAX&#95;INT) como valor abierto.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY somedict(
        Abcdef UInt64,
        StartTimeStamp UInt64,
        EndTimeStamp UInt64,
        XXXType String DEFAULT ''
    )
    PRIMARY KEY Abcdef
    RANGE(MIN StartTimeStamp MAX EndTimeStamp)
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <clickhouse>
        <dictionary>
            ...

            <layout>
                <range_hashed />
            </layout>

            <structure>
                <id>
                    <name>Abcdef</name>
                </id>
                <range_min>
                    <name>StartTimeStamp</name>
                    <type>UInt64</type>
                </range_min>
                <range_max>
                    <name>EndTimeStamp</name>
                    <type>UInt64</type>
                </range_max>
                <attribute>
                    <name>XXXType</name>
                    <type>String</type>
                    <null_value />
                </attribute>
            </structure>

        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

Ejemplo de configuración con rangos superpuestos y rangos abiertos:

```sql
CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

SELECT * FROM discounts ORDER BY advertiser_id, discount_start_date;
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│             1 │          2015-01-01 │              ᴺᵁᴸᴸ │    0.1 │
│             1 │          2015-01-15 │              ᴺᵁᴸᴸ │    0.2 │
│             2 │          2015-01-01 │        2015-01-15 │    0.3 │
│             2 │          2015-01-04 │        2015-01-10 │    0.4 │
│             3 │          1970-01-01 │        2015-01-15 │    0.5 │
│             3 │          1970-01-01 │        2015-01-10 │    0.6 │
└───────────────┴─────────────────────┴───────────────────┴────────┘

-- RANGE_LOOKUP_STRATEGY 'max'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.2 │ -- two ranges are matching, range_min 2015-01-15 (0.2) is bigger than 2015-01-01 (0.1)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.4 │ -- two ranges are matching, range_min 2015-01-04 (0.4) is bigger than 2015-01-01 (0.3)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.5 │ -- two ranges are matching, range_min are equal, 2015-01-15 (0.5) is bigger than 2015-01-10 (0.6)
└─────┘

DROP DICTIONARY discounts_dict;

-- RANGE_LOOKUP_STRATEGY 'min'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.1 │ -- two ranges are matching, range_min 2015-01-01 (0.1) is less than 2015-01-15 (0.2)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.3 │ -- two ranges are matching, range_min 2015-01-01 (0.3) is less than 2015-01-04 (0.4)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.6 │ -- two ranges are matching, range_min are equal, 2015-01-10 (0.6) is less than 2015-01-15 (0.5)
└─────┘
```

<div id="complex_key_range_hashed">
  ## complex_key_range_hashed
</div>

El diccionario se almacena en memoria en forma de una tabla hash con un array ordenado de rangos y sus valores correspondientes (consulte [range&#95;hashed](#range_hashed)). Este tipo de almacenamiento se utiliza para [claves](../attributes.md#composite-key) compuestas.

Ejemplo de configuración:

```sql
CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  CountryKey String,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);
```