---
description: 'Documentation d''ALTER'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

La plupart des requêtes `ALTER TABLE` modifient les paramètres de la table ou les données :

| Modificateur                                                                |
| --------------------------------------------------------------------------- |
| [COLUMN](/fr/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/fr/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/fr/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/fr/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/fr/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/fr/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/fr/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/fr/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/fr/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/fr/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/fr/sql-reference/statements/alter/apply-patches.md)           |

:::note
La plupart des requêtes `ALTER TABLE` ne sont prises en charge que pour les tables [*MergeTree](/fr/engines/table-engines/mergetree-family/index.md), [Merge](/fr/engines/table-engines/special/merge.md) et [Distributed](/fr/engines/table-engines/special/distributed.md).
:::

Ces instructions `ALTER` concernent les vues :

| Instruction                                                             | Description                                                                               |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY QUERY](/fr/sql-reference/statements/alter/view.md) | Modifie la structure d&#39;une [vue matérialisée](/fr/sql-reference/statements/create/view). |

Ces instructions `ALTER` modifient les entités liées au contrôle d&#39;accès basé sur les rôles :

| Instruction                                                             |
| ----------------------------------------------------------------------- |
| [USER](/fr/sql-reference/statements/alter/user.md)                         |
| [ROLE](/fr/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/fr/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/fr/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/fr/sql-reference/statements/alter/settings-profile.md) |

| Instruction                                                                   | Description                                                                                               |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/fr/sql-reference/statements/alter/comment.md)  | Ajoute, modifie ou supprime les commentaires de la table, qu&#39;ils aient été définis auparavant ou non. |
| [ALTER NAMED COLLECTION](/fr/sql-reference/statements/alter/named-collection.md) | Modifie les [collections nommées](/fr/operations/named-collections.md).                                      |

<div id="mutations">
  ## Mutations
</div>

Les requêtes `ALTER` destinées à modifier les données des tables sont mises en œuvre via un mécanisme appelé « mutations », notamment [ALTER TABLE ... DELETE](/fr/sql-reference/statements/alter/delete.md) et [ALTER TABLE ... UPDATE](/fr/sql-reference/statements/alter/update.md). Il s’agit de processus asynchrones exécutés en arrière-plan, semblables aux fusions dans les tables [MergeTree](/fr/engines/table-engines/mergetree-family/index.md), qui produisent de nouvelles versions « mutées » des parts.

Pour les tables `*MergeTree`, les mutations s’exécutent en **réécrivant des parts de données entières**.
Il n’y a pas d’atomicité : les parts sont remplacées par leurs versions mutées dès qu’elles sont prêtes, et une requête `SELECT` qui commence à s’exécuter pendant une mutation verra à la fois des données provenant de parts déjà mutées et de parts qui ne l’ont pas encore été.

Les mutations sont totalement ordonnées selon leur ordre de création et sont appliquées à chaque part dans cet ordre. Les mutations sont également partiellement ordonnées par rapport aux requêtes `INSERT INTO` : les données insérées dans la table avant la soumission de la mutation seront mutées, tandis que celles insérées après ne le seront pas. Notez que les mutations ne bloquent en aucune façon les insertions.

Une requête de mutation renvoie immédiatement après l’ajout de l’entrée de mutation (dans le cas des tables répliquées, dans ZooKeeper ; pour les tables non répliquées, dans le système de fichiers). La mutation elle-même s’exécute de manière asynchrone en utilisant les paramètres du profil système. Pour suivre la progression des mutations, vous pouvez utiliser la table [`system.mutations`](/fr/operations/system-tables/mutations). Une mutation soumise avec succès continuera à s’exécuter même si les serveurs ClickHouse sont redémarrés. Il n’existe aucun moyen de revenir sur une mutation une fois qu’elle a été soumise, mais si elle reste bloquée pour une raison quelconque, elle peut être annulée avec la requête [`KILL MUTATION`](/fr/sql-reference/statements/kill.md/#kill-mutation).

Les entrées correspondant aux mutations terminées ne sont pas supprimées immédiatement (le nombre d’entrées conservées est déterminé par le paramètre du moteur de stockage `finished_mutations_to_keep`). Les entrées de mutation les plus anciennes sont supprimées.

<div id="synchronicity-of-alter-queries">
  ## Exécution synchrone des requêtes ALTER
</div>

Pour les tables non répliquées, toutes les requêtes `ALTER` sont exécutées de façon synchrone. Pour les tables répliquées, la requête se contente d’ajouter dans `ZooKeeper` des instructions correspondant aux actions à effectuer, lesquelles sont ensuite exécutées dès que possible. Cependant, la requête peut attendre que ces actions soient terminées sur toutes les répliques.

Pour les requêtes `ALTER` qui créent des mutations (par exemple, notamment `UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `APPLY PATCHES`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC`), le caractère synchrone est défini par le paramètre [mutations&#95;sync](/fr/operations/settings/settings.md/#mutations_sync).

Pour les autres requêtes `ALTER` qui modifient uniquement les métadonnées, vous pouvez utiliser le paramètre [alter&#95;sync](/fr/operations/settings/settings#alter_sync) pour configurer l’attente.

Vous pouvez spécifier combien de temps (en secondes) attendre que les répliques inactives exécutent toutes les requêtes `ALTER` à l’aide du paramètre [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/fr/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Pour toutes les requêtes `ALTER`, si `alter_sync = 2` et que certaines répliques restent inactives plus longtemps que la durée spécifiée dans le paramètre `replication_wait_for_inactive_replica_timeout`, une exception `UNFINISHED` est levée.
:::

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Gérer les mises à jour et les suppressions dans ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)