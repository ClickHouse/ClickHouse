---
description: 'Crée une table temporaire de la structure spécifiée avec le moteur de table
  Null. La fonction est utilisée pour faciliter l''écriture de tests et les démonstrations.'
sidebar_label: 'fonction null'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'référence'
---

Crée une table temporaire de la structure spécifiée avec le moteur de table [Null](../../engines/table-engines/special/null.md). D’après les propriétés du moteur `Null`, les données de la table sont ignorées et la table elle-même est immédiatement supprimée après l’exécution de la requête. La fonction est utilisée pour faciliter l’écriture de tests et les démonstrations.

<div id="syntax">
  ## Syntaxe
</div>

```sql
null('structure')
```

<div id="argument">
  ## Argument
</div>

* `structure` — Une liste de colonnes et de leurs types. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valeur renvoyée
</div>

Une table temporaire à moteur `Null` avec la structure spécifiée.

<div id="example">
  ## Exemple
</div>

Requête avec la fonction `null` :

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

peut remplacer trois requêtes :

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## Voir aussi
</div>

* [Moteur de table Null](../../engines/table-engines/special/null.md)