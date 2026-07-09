---
description: 'Documentation du type de données Date32 dans ClickHouse, qui stocke des dates
  sur une plage plus étendue que Date'
sidebar_label: 'Date32'
sidebar_position: 14
slug: /sql-reference/data-types/date32
title: 'Date32'
doc_type: 'reference'
---

Une date. Prend en charge la même plage de dates que [DateTime64](../../sql-reference/data-types/datetime64.md). Elle est stockée sous la forme d&#39;un entier signé de 32 bits, dans l&#39;ordre natif des octets, la valeur représentant le nombre de jours écoulés depuis `1900-01-01`. **Important !** 0 représente `1970-01-01`, et les valeurs négatives représentent les jours antérieurs à `1970-01-01`.

**Exemples**

Création d&#39;une table avec une colonne de type `Date32` et insertion de données :

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**Voir aussi**

* [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
* [toDate32OrZero](/fr/sql-reference/functions/type-conversion-functions#toDate32OrZero)
* [toDate32OrNull](/fr/sql-reference/functions/type-conversion-functions#toDate32OrNull)