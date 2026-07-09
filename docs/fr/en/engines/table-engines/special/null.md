---
description: 'Lors de l’écriture dans une table `Null`, les données sont ignorées. Lors de la lecture d’une
  table `Null`, la réponse est vide.'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Moteur de table Null'
doc_type: 'reference'
---

Lors de l’écriture de données dans une table `Null`, les données sont ignorées.
Lors de la lecture d’une table `Null`, la réponse est vide.

Le moteur de table `Null` est utile pour les transformations de données lorsque vous n’avez plus besoin des données d’origine une fois celles-ci transformées.
À cette fin, vous pouvez créer une vue matérialisée sur une table `Null`.
Les données écrites dans la table seront consommées par la vue, mais les données brutes d’origine seront ignorées.