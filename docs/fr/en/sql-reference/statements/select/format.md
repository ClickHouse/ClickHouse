---
description: 'Documentation de la clause FORMAT'
sidebar_label: 'FORMAT'
slug: /sql-reference/statements/select/format
title: 'Clause FORMAT'
doc_type: 'reference'
---

ClickHouse prend en charge un large éventail de [formats de sérialisation](../../../interfaces/formats.md), utilisables notamment pour le résultat de la requête. Il existe plusieurs façons de choisir un format pour la sortie de `SELECT` ; l&#39;une d&#39;elles consiste à indiquer `FORMAT format` à la fin de la requête afin d&#39;obtenir les données dans un format spécifique.

Un format spécifique peut être utilisé pour des raisons pratiques, pour l&#39;intégration à d&#39;autres systèmes ou pour améliorer les performances.

<div id="default-format">
  ## Format par défaut
</div>

Si la clause `FORMAT` est omise, le format par défaut est utilisé ; celui-ci dépend à la fois des paramètres et de l’interface utilisée pour accéder au serveur ClickHouse. Pour l’[interface HTTP](/fr/interfaces/http) et le [client en ligne de commande](../../../interfaces/client.md) en mode batch, le format par défaut est `TabSeparated`. Pour le client en ligne de commande en mode interactif, le format par défaut est `PrettyCompact` (il produit des tables compactes lisibles par l’homme).

<div id="implementation-details">
  ## Détails d’implémentation
</div>

Lors de l’utilisation du client en ligne de commande, les données sont toujours transmises via le réseau dans un format interne efficace (`Native`). Le client interprète lui-même la clause `FORMAT` de la requête et formate lui-même les données, ce qui évite une charge supplémentaire sur le réseau et le serveur.