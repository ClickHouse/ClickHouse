---
description: 'Documentation de la clause UNION'
sidebar_label: 'UNION'
slug: /sql-reference/statements/select/union
title: 'Clause UNION'
doc_type: 'reference'
---

Vous pouvez utiliser `UNION` en spécifiant explicitement `UNION ALL` ou `UNION DISTINCT`.

Si vous ne spécifiez ni `ALL` ni `DISTINCT`, le comportement dépend du paramètre `union_default_mode`. La différence entre `UNION ALL` et `UNION DISTINCT` est que `UNION DISTINCT` supprime les doublons dans le résultat de l’union ; cela équivaut à un `SELECT DISTINCT` à partir d’une sous-requête contenant `UNION ALL`.

Vous pouvez utiliser `UNION` pour combiner un nombre quelconque de requêtes `SELECT` en réunissant leurs résultats. Exemple :

```sql title="Query"
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Les colonnes du résultat sont mises en correspondance selon leur index (ordre dans `SELECT`). Si les noms de colonnes ne correspondent pas, les noms du résultat final sont repris de la première requête.

Le transtypage est effectué pour les unions. Par exemple, si deux requêtes combinées ont le même champ avec des types `Nullable` et non-`Nullable` d’un type compatible, le `UNION` résultant aura un champ de type `Nullable`.

Les requêtes faisant partie de `UNION` peuvent être placées entre `()`. [ORDER BY](../../../sql-reference/statements/select/order-by.md) et [LIMIT](../../../sql-reference/statements/select/limit.md) s’appliquent à chaque requête séparément, et non au résultat final. Si vous devez appliquer une transformation au résultat final, vous pouvez placer toutes les requêtes avec `UNION` dans une sous-requête de la clause [FROM](../../../sql-reference/statements/select/from.md).

Si vous utilisez `UNION` sans spécifier explicitement `UNION ALL` ou `UNION DISTINCT`, vous pouvez définir le mode d’union à l’aide du paramètre [union&#95;default&#95;mode](/fr/operations/settings/settings#union_default_mode). Les valeurs du paramètre peuvent être `ALL`, `DISTINCT` ou une chaîne vide. Cependant, si vous utilisez `UNION` avec le paramètre `union_default_mode` défini sur une chaîne vide, une exception sera levée. Les exemples suivants montrent les résultats des requêtes pour différentes valeurs du paramètre.

```sql title="Query"
SET union_default_mode = 'DISTINCT';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

```sql title="Query"
SET union_default_mode = 'ALL';
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 2;
```

```text title="Response"
┌─1─┐
│ 1 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 2 │
└───┘
┌─1─┐
│ 3 │
└───┘
```

Les requêtes faisant partie de `UNION/UNION ALL/UNION DISTINCT` peuvent être exécutées simultanément, et leurs résultats peuvent être combinés.

**Voir aussi**

* paramètre [insert&#95;null&#95;as&#95;default](../../../operations/settings/settings.md#insert_null_as_default).
* paramètre [union&#95;default&#95;mode](/fr/operations/settings/settings#union_default_mode).