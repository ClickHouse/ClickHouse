---
slug: /ru/operations/system-tables/contributors
---
# system.contributors {#system-contributors}

Содержит информацию о контрибьютерах (авторов коммитов) из git log в случайном порядке. Порядок определяется заново при каждом запросе.

Столбцы:

-   `name` ([String](../../sql-reference/data-types/string.md)) — Имя контрибьютера.
-   `email` ([String](../../sql-reference/data-types/string.md)) — Электронная почта контрибьютера.
-   `commits` ([UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Количество коммитов контрибьютера.

**Пример**

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

Чтобы найти себя в таблице, выполните запрос:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┬─email───────────────────────────────────┬─commits─┐
│ Olga Khvostikova │ Wireless-fidelity@yandex.ru             │       2 │
│ Olga Khvostikova │ insubconsciousness@gmail.com            │      28 │
│ Olga Khvostikova │ khvostikovao@gborisenko.haze.yandex.net │       3 │
└──────────────────┴─────────────────────────────────────────┴─────────┘
```
