---
description: 'Documentation de CHECK GRANT'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'Instruction CHECK GRANT'
doc_type: 'reference'
---

La requête `CHECK GRANT` permet de vérifier si l’utilisateur courant ou le rôle courant dispose d’un privilège spécifique.

<div id="syntax">
  ## Syntaxe
</div>

La syntaxe de base de la requête est la suivante :

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — Type de privilège.

<div id="examples">
  ## Exemples
</div>

Si ce privilège a déjà été accordé à l’utilisateur, la réponse `check_grant` sera `1`. Sinon, la réponse `check_grant` sera `0`.

Si `table_1.col1` existe et que l’utilisateur courant dispose du privilège `SELECT`/`SELECT(con)` ou d’un rôle (incluant ce privilège), la réponse est `1`.

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

Si `table_2.col2` n&#39;existe pas, ou si l’utilisateur courant n&#39;a pas reçu le privilège `SELECT`/`SELECT(con)` ni un rôle (avec ce privilège), la réponse est `0`.

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## Caractère générique
</div>

Lors de la spécification des privilèges, vous pouvez utiliser un astérisque (`*`) à la place d’un nom de table ou de base de données. Consultez [WILDCARD GRANTS](../../sql-reference/statements/grant.md#wildcard-grants) pour les règles d’utilisation des caractères génériques.