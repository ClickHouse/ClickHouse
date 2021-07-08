---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# FORMAT de la Clause {#format-clause}

Clickhouse prend en charge une large gamme de [formats de sérialisation](../../../interfaces/formats.md) qui peut être utilisé sur les résultats de la requête entre autres choses. Il existe plusieurs façons de choisir un format pour `SELECT` de sortie, l'un d'eux est de spécifier `FORMAT format` à la fin de la requête pour obtenir les données résultantes dans tout format spécifique.

Un format spécifique peut être utilisé pour des raisons de commodité, d'intégration avec d'autres systèmes ou d'amélioration des performances.

## Format Par Défaut {#default-format}

Si l' `FORMAT` la clause est omise, le format par défaut est utilisé, ce qui dépend à la fois des paramètres et de l'interface utilisée pour accéder au serveur ClickHouse. Pour l' [Interface HTTP](../../../interfaces/http.md) et la [client de ligne de commande](../../../interfaces/cli.md) en mode batch, le format par défaut est `TabSeparated`. Pour le client de ligne de commande en mode interactif, le format par défaut est `PrettyCompact` (il produit des tables compactes lisibles par l'homme).

## Détails De Mise En Œuvre {#implementation-details}

Lors de l'utilisation du client de ligne de commande, les données sont toujours transmises sur le réseau dans un format efficace interne (`Native`). Le client interprète indépendamment le `FORMAT` clause de la requête et formate les données elles-mêmes (soulageant ainsi le réseau et le serveur de la charge supplémentaire).
