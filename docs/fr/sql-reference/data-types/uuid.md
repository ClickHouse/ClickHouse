---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

Un identifiant unique universel (UUID) est un numéro de 16 octets utilisé pour identifier les enregistrements. Pour plus d'informations sur L'UUID, voir [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

L'exemple de valeur de type UUID est représenté ci-dessous:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

Si vous ne spécifiez pas la valeur de la colonne UUID lors de l'insertion d'un nouvel enregistrement, la valeur UUID est remplie avec zéro:

``` text
00000000-0000-0000-0000-000000000000
```

## Comment générer {#how-to-generate}

Pour générer la valeur UUID, ClickHouse fournit [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) fonction.

## Exemple D'Utilisation {#usage-example}

**Exemple 1**

Cet exemple montre la création d'une table avec la colonne de type UUID et l'insertion d'une valeur dans la table.

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Exemple 2**

Dans cet exemple, la valeur de la colonne UUID n'est pas spécifiée lors de l'insertion d'un nouvel enregistrement.

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## Restriction {#restrictions}

Le type de données UUID ne prend en charge que les fonctions qui [Chaîne](string.md) type de données prend également en charge (par exemple, [min](../../sql-reference/aggregate-functions/reference.md#agg_function-min), [Max](../../sql-reference/aggregate-functions/reference.md#agg_function-max), et [compter](../../sql-reference/aggregate-functions/reference.md#agg_function-count)).

Le type de données UUID n'est pas pris en charge par les opérations arithmétiques (par exemple, [ABS](../../sql-reference/functions/arithmetic-functions.md#arithm_func-abs)) ou des fonctions d'agrégation, comme [somme](../../sql-reference/aggregate-functions/reference.md#agg_function-sum) et [avg](../../sql-reference/aggregate-functions/reference.md#agg_function-avg).

[Article Original](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
