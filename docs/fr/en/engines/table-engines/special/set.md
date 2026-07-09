---
description: 'Un ensemble de données toujours en RAM. Il est destiné à être utilisé du côté droit
  de l’opérateur `IN`.'
sidebar_label: 'Set'
sidebar_position: 60
slug: /engines/table-engines/special/set
title: 'Moteur de table Set'
doc_type: 'reference'
---

:::note
Dans ClickHouse Cloud, si votre service a été créé avec une version antérieure à 25.4, vous devrez définir la compatibilité sur au moins 25.4 à l’aide de `SET compatibility=25.4`.
:::

Un ensemble de données toujours en RAM. Il est destiné à être utilisé du côté droit de l’opérateur `IN` (voir la section « opérateurs IN »).

Vous pouvez utiliser `INSERT` pour insérer des données dans la table. Les nouveaux éléments seront ajoutés à l’ensemble de données, tandis que les doublons seront ignorés.
Mais vous ne pouvez pas effectuer de `SELECT` sur la table. La seule manière de récupérer les données est de l’utiliser dans la partie droite de l’opérateur `IN`.

Les données se trouvent toujours en RAM. Pour `INSERT`, les blocs de données insérées sont également écrits dans le répertoire des tables sur le disque. Au démarrage du serveur, ces données sont chargées en RAM. En d’autres termes, après un redémarrage, les données sont conservées.

Lors d’un redémarrage brutal du serveur, le bloc de données sur le disque peut être perdu ou endommagé. Dans ce dernier cas, vous devrez peut-être supprimer manuellement le fichier contenant les données endommagées.

<div id="join-limitations-and-settings">
  ### Limitations et paramètres
</div>

Lors de la création d&#39;une table, les paramètres suivants sont appliqués :

<div id="persistent">
  #### Persistent
</div>

Désactive la persistance pour les moteurs de table Set et [Join](/fr/engines/table-engines/special/join).

Réduit la surcharge liée aux E/S. Convient aux scénarios qui privilégient les performances et ne nécessitent pas de persistance.

Valeurs possibles :

* 1 — Activé.
* 0 — Désactivé.

Valeur par défaut : `1`.