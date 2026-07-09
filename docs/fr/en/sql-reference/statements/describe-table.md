---
description: 'Documentation de DESCRIBE TABLE'
sidebar_label: 'DESCRIBE TABLE'
sidebar_position: 42
slug: /sql-reference/statements/describe-table
title: 'DESCRIBE TABLE'
doc_type: 'reference'
---

Renvoie des informations sur les colonnes de la table.

**Syntaxe**

```sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

L’instruction `DESCRIBE` renvoie une ligne pour chaque colonne de la table, avec les valeurs [String](../../sql-reference/data-types/string.md) suivantes :

* `name` — Le nom d’une colonne.
* `type` — Le type d’une colonne.
* `default_type` — Une clause utilisée dans l’[expression par défaut de la colonne](/fr/sql-reference/statements/create/table) : `DEFAULT`, `MATERIALIZED` ou `ALIAS`. S’il n’y a pas d’expression par défaut, une chaîne vide est renvoyée.
* `default_expression` — Une expression spécifiée après la clause `DEFAULT`.
* `comment` — Un [commentaire de colonne](/fr/sql-reference/statements/alter/column#comment-column).
* `codec_expression` — Un [codec](/fr/sql-reference/statements/create/table#column_compression_codec) appliqué à la colonne.
* `ttl_expression` — Une expression [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
* `is_subcolumn` — Un indicateur qui vaut `1` pour les sous-colonnes internes. Il n’est inclus dans le résultat que si la description des sous-colonnes est activée via le paramètre [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

Toutes les colonnes des structures de données [Nested](../../sql-reference/data-types/nested-data-structures/index.md) sont décrites séparément. Le nom de chaque colonne est préfixé par le nom de la colonne parente, suivi d’un point.

Pour afficher les sous-colonnes internes des autres types de données, utilisez le paramètre [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).

**Exemple**

```sql title="Query"
CREATE TABLE describe_example (
    id UInt64, text String DEFAULT 'unknown' CODEC(ZSTD),
    user Tuple (name String, age UInt8)
) ENGINE = MergeTree() ORDER BY id;

DESCRIBE TABLE describe_example;
DESCRIBE TABLE describe_example SETTINGS describe_include_subcolumns=1;
```

```text title="Response"
┌─name─┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ id   │ UInt64                        │              │                    │         │                  │                │
│ text │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │
│ user │ Tuple(name String, age UInt8) │              │                    │         │                  │                │
└──────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

La deuxième requête affiche également des sous-colonnes :

```text title="Response"
┌─name──────┬─type──────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┬─is_subcolumn─┐
│ id        │ UInt64                        │              │                    │         │                  │                │            0 │
│ text      │ String                        │ DEFAULT      │ 'unknown'          │         │ ZSTD(1)          │                │            0 │
│ user      │ Tuple(name String, age UInt8) │              │                    │         │                  │                │            0 │
│ user.name │ String                        │              │                    │         │                  │                │            1 │
│ user.age  │ UInt8                         │              │                    │         │                  │                │            1 │
└───────────┴───────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┴──────────────┘
```

L’instruction DESCRIBE peut également être utilisée avec des sous-requêtes ou des expressions scalaires :

```SQL
DESCRIBE SELECT 1 FORMAT TSV;
```

ou

```SQL
DESCRIBE (SELECT 1) FORMAT TSV;
```

```text title="Response"
1       UInt8
```

Cette syntaxe renvoie des métadonnées sur les colonnes de résultat de la requête ou de la sous-requête spécifiée. Elle est utile pour comprendre la structure de requêtes complexes avant leur exécution.

**Voir aussi**

* Le paramètre [describe&#95;include&#95;subcolumns](../../operations/settings/settings.md#describe_include_subcolumns).