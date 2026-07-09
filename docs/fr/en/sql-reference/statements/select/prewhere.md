---
description: 'Documentation de la clause PREWHERE'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'Clause PREWHERE'
doc_type: 'référence'
---

Prewhere est une optimisation qui permet d&#39;appliquer le filtrage plus efficacement. Elle est activée par défaut, même si la clause `PREWHERE` n&#39;est pas spécifiée explicitement. Elle fonctionne en déplaçant automatiquement une partie de la condition [WHERE](../../../sql-reference/statements/select/where.md) vers l&#39;étape prewhere. Le rôle de la clause `PREWHERE` est uniquement de contrôler cette optimisation si vous pensez pouvoir faire mieux que le comportement par défaut.

Avec l&#39;optimisation prewhere, seules les colonnes nécessaires à l&#39;exécution de l&#39;expression prewhere sont d&#39;abord lues. Ensuite, les autres colonnes nécessaires à l&#39;exécution du reste de la requête sont lues, mais uniquement pour les blocs où l&#39;expression prewhere est `true` pour au moins quelques lignes. S&#39;il existe beaucoup de blocs où l&#39;expression prewhere est `false` pour toutes les lignes et que prewhere nécessite moins de colonnes que les autres parties de la requête, cela permet souvent de lire beaucoup moins de données depuis le disque lors de l&#39;exécution de la requête.

<div id="controlling-prewhere-manually">
  ## Contrôle manuel de PREWHERE
</div>

Cette clause a le même rôle que la clause `WHERE`. La différence tient aux données lues dans la table. Le contrôle manuel de `PREWHERE` est utile pour des conditions de filtrage utilisées par un petit nombre de colonnes dans la requête, mais offrant un fort pouvoir de filtrage des données. Cela réduit le volume de données à lire.

Une requête peut spécifier simultanément `PREWHERE` et `WHERE`. Dans ce cas, `PREWHERE` est appliquée avant `WHERE`.

Si le paramètre [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) est défini sur 0, les heuristiques qui déplacent automatiquement des parties d’expressions de `WHERE` vers `PREWHERE` sont désactivées.

Si la requête utilise le modificateur [FINAL](/fr/sql-reference/statements/select/from#final-modifier), l’optimisation `PREWHERE` n’est pas toujours correcte. Elle n’est activée que si les deux paramètres [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) et [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) sont activés.

:::note
La section `PREWHERE` est exécutée avant `FINAL`. Les résultats des requêtes `FROM ... FINAL` peuvent donc être faussés lors de l’utilisation de `PREWHERE` avec des champs qui ne figurent pas dans la section `ORDER BY` d’une table.
:::

<div id="limitations">
  ## Limites
</div>

`PREWHERE` est uniquement pris en charge par les tables de la famille [*MergeTree](../../../engines/table-engines/mergetree-family/index.md).

<div id="example">
  ## Exemple
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```