---
description: 'Page de vue d’ensemble des paramètres.'
sidebar_position: 1
slug: /operations/settings/overview
title: 'Vue d’ensemble des paramètres'
doc_type: 'reference'
---

<div id="overview">
  ## Vue d’ensemble
</div>

:::note
Les profils de paramètres basés sur XML et les [fichiers de configuration](/fr/operations/configuration-files) ne sont actuellement pas pris en charge dans ClickHouse Cloud. Pour définir des paramètres pour votre service ClickHouse Cloud, vous devez utiliser les [profils de paramètres pilotés par SQL](/fr/operations/access-rights#settings-profiles-management).
:::

Voici les principaux groupes de paramètres ClickHouse :

* Paramètres globaux du serveur
* Paramètres de session
* Paramètres de requête
* Paramètres des opérations en arrière-plan

Les paramètres globaux s’appliquent par défaut, sauf s’ils sont remplacés à des niveaux plus spécifiques. Les paramètres de session peuvent être définis via des profils, la configuration des utilisateurs et des commandes SET. Les paramètres de requête peuvent être fournis via la clause SETTINGS et s’appliquent à chaque requête individuellement. Les paramètres des opérations en arrière-plan s’appliquent aux mutations, aux fusions et éventuellement à d’autres opérations exécutées de manière asynchrone en arrière-plan.

<div id="see-non-default-settings">
  ## Afficher les paramètres non définis par défaut
</div>

Pour voir quels paramètres ont été modifiés par rapport à leur valeur par défaut, vous pouvez interroger la
table `system.settings` :

```sql
SELECT name, value FROM system.settings WHERE changed
```

Si aucun paramètre n’a été modifié par rapport à sa valeur par défaut, ClickHouse ne
renverra rien.

Pour vérifier la valeur d’un paramètre donné, vous pouvez spécifier le `name` du
paramètre dans votre requête :

```sql
SELECT name, value FROM system.settings WHERE name = 'max_threads'
```

Ce qui renverra quelque chose comme ceci :

```response
┌─name────────┬─value───┐
│ max_threads │ auto(8) │
└─────────────┴─────────┘

1 row in set. Elapsed: 0.002 sec.
```

<div id="further-reading">
  ## Pour aller plus loin
</div>

* Consultez [les paramètres globaux du serveur](/fr/operations/server-configuration-parameters/settings.md) pour en savoir plus sur la configuration de votre
  serveur ClickHouse à l’échelle globale du serveur.
* Consultez [les paramètres de session](/fr/operations/settings/settings-query-level.md) pour en savoir plus sur la configuration de votre serveur ClickHouse
  au niveau de la session.
* Consultez [la hiérarchie des contextes](/fr/development/architecture.md#context) pour en savoir plus sur le traitement de la configuration par ClickHouse.