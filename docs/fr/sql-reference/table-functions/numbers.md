---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: nombre
---

# nombre {#numbers}

`numbers(N)` – Returns a table with the single ‘number’ colonne (UInt64) qui contient des entiers de 0 à n-1.
`numbers(N, M)` - Retourne un tableau avec le seul ‘number’ colonne (UInt64) qui contient des entiers de N À (N + M-1).

Similaire à la `system.numbers` table, il peut être utilisé pour tester et générer des valeurs successives, `numbers(N, M)` plus efficace que `system.numbers`.

Les requêtes suivantes sont équivalentes:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

Exemple:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/numbers/) <!--hide-->
