---
slug: /en/operations/system-tables/contributors
---
# contributors

Contains information about contributors (authors) from the git log. The order is random at query execution time.

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Contributor's name.
- `email` ([String](../../sql-reference/data-types/string.md)) — Contributor's email from.
- `commits` ([UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of contributor's commits.

**Example**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name────────────┬─email─────────────────────────────────────────┬─commits─┐
│ Aleksandr Karo  │ alex@karo-dev.ru                              │       1 │
│ andrei-karpliuk │ karpliuk.a@profitero.com                      │       1 │
│ Tyler Hannan    │ tyler@clickhouse.com                          │      25 │
│ Brendan Cox     │ justnoise@gmail.com                           │       1 │
│ alekseik1       │ 1alekseik1@gmail.com                          │       1 │
│ Ladislav Snizek │ ladislav.snizek@cdn77.com                     │       3 │
│ Pavel Kruglov   │ avogar@sandbox-633380738                      │     198 │
│ f1yegor         │ f1yegor@gmail.com                             │      24 │
│ stan            │ 31004541+YunlongChen@users.noreply.github.com │       2 │
│ Julian Maicher  │ jmaicher@users.noreply.github.com             │       1 │
└─────────────────┴───────────────────────────────────────────────┴─────────┘
```

To find out yourself in the table, use a query:

``` sql
SELECT * FROM system.contributors WHERE name = 'Olga Khvostikova'
```

``` text
┌─name─────────────┬─email───────────────────────────────────┬─commits─┐
│ Olga Khvostikova │ Wireless-fidelity@yandex.ru             │       2 │
│ Olga Khvostikova │ insubconsciousness@gmail.com            │      28 │
│ Olga Khvostikova │ khvostikovao@gborisenko.haze.yandex.net │       3 │
└──────────────────┴─────────────────────────────────────────┴─────────┘
```
