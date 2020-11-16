---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause PREWHERE {#prewhere-clause}

Prewhere est une optimisation pour appliquer le filtrage plus efficacement. Il est activé par défaut, même si `PREWHERE` la clause n'est pas explicitement spécifié. Il fonctionne en déplaçant automatiquement une partie de [WHERE](where.md) condition à prewhere étape. Le rôle de `PREWHERE` la clause est seulement pour contrôler cette optimisation si vous pensez que vous savez comment le faire mieux que par défaut.

Avec l'optimisation prewhere, au début, seules les colonnes nécessaires à l'exécution de l'expression prewhere sont lues. Ensuite, les autres colonnes sont lues qui sont nécessaires pour exécuter le reste de la requête, mais seulement les blocs où l'expression prewhere est “true” au moins pour certaines lignes. S'il y a beaucoup de blocs où prewhere expression est “false” pour toutes les lignes et prewhere a besoin de moins de colonnes que les autres parties de la requête, cela permet souvent de lire beaucoup moins de données à partir du disque pour l'exécution de la requête.

## Contrôle Manuel De Prewhere {#controlling-prewhere-manually}

La clause a le même sens que la `WHERE` clause. La différence est dans laquelle les données sont lues à partir de la table. Quand à commander manuellement `PREWHERE` pour les conditions de filtration qui sont utilisées par une minorité des colonnes de la requête, mais qui fournissent une filtration de données forte. Cela réduit le volume de données à lire.

Une requête peut spécifier simultanément `PREWHERE` et `WHERE`. Dans ce cas, `PREWHERE` précéder `WHERE`.

Si l' `optimize_move_to_prewhere` le paramètre est défini sur 0, heuristiques pour déplacer automatiquement des parties d'expressions `WHERE` de `PREWHERE` sont désactivés.

## Limitation {#limitations}

`PREWHERE` est uniquement pris en charge par les tables `*MergeTree` famille.
