---
description: 'Documentation de FUNCTION'
sidebar_label: 'FUNCTION'
sidebar_position: 38
slug: /sql-reference/statements/create/function
title: 'CREATE FUNCTION - fonction définie par l’utilisateur (UDF)'
doc_type: 'reference'
---

Crée une fonction définie par l’utilisateur (UDF) à partir d’une expression lambda. L’expression doit se composer de paramètres de fonction, de constantes, d’opérateurs ou d’autres appels de fonction.

**Syntaxe**

```sql
CREATE [OR REPLACE] FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression
```

Une fonction peut avoir un nombre quelconque de paramètres.

Il existe quelques restrictions :

* Le nom d’une fonction doit être unique parmi les fonctions définies par l’utilisateur et les fonctions système.
* Les fonctions récursives ne sont pas autorisées.
* Toutes les variables utilisées par une fonction doivent figurer dans sa liste de paramètres.

Si l’une de ces restrictions n’est pas respectée, une exception est levée.

**Exemple**

```sql title="Query"
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

```text title="Response"
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

Une [fonction conditionnelle](../../../sql-reference/functions/conditional-functions.md) est appelée dans une fonction définie par l’utilisateur, dans la requête suivante :

```sql title="Query"
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

```text title="Response"
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```

Remplacez une UDF existante :

```sql title="Query"
CREATE FUNCTION exampleReplaceFunction AS frame -> frame;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
CREATE OR REPLACE FUNCTION exampleReplaceFunction AS frame -> frame + 1;
SELECT create_query FROM system.functions WHERE name = 'exampleReplaceFunction';
```

```text title="Response"
┌─create_query─────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> frame │
└──────────────────────────────────────────────────────────┘

┌─create_query───────────────────────────────────────────────────┐
│ CREATE FUNCTION exampleReplaceFunction AS frame -> (frame + 1) │
└────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## Contenu connexe
</div>

<div id="executable-udfs">
  ### [UDF exécutables](/fr/sql-reference/functions/udf.md).
</div>

<div id="user-defined-functions-in-clickhouse-cloud">
  ### [Fonctions définies par l’utilisateur dans ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)
</div>
