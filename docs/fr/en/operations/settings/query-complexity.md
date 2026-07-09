---
description: 'Paramètres limitant la complexité des requêtes.'
sidebar_label: 'Restrictions sur la complexité des requêtes'
sidebar_position: 59
slug: /operations/settings/query-complexity
title: 'Restrictions sur la complexité des requêtes'
doc_type: 'référence'
---

<div id="overview">
  ## Vue d’ensemble
</div>

Dans le cadre des [paramètres](/fr/operations/settings/overview), ClickHouse offre
la possibilité d’imposer des restrictions sur la complexité des requêtes. Cela permet de se prémunir contre des
requêtes potentiellement gourmandes en ressources, en garantissant une exécution plus sûre et plus prévisible,
en particulier lors de l’utilisation de l’interface utilisateur.

Presque toutes les restrictions s’appliquent uniquement aux requêtes `SELECT`, et pour le
traitement distribué des requêtes, elles sont appliquées séparément sur chaque serveur.

ClickHouse ne vérifie généralement ces restrictions qu’après le traitement complet des parties de données,
plutôt que pour chaque ligne. Il peut donc arriver que ces restrictions soient dépassées pendant le traitement
d’une partie.

<div id="overflow_mode_setting">
  ## Paramètres de `overflow_mode`
</div>

La plupart des restrictions disposent également d’un paramètre `overflow_mode`, qui définit ce qui se passe
lorsque la limite est dépassée, et peut prendre l’une des deux valeurs suivantes :

* `throw` : lever une exception (par défaut).
* `break` : arrêter l’exécution de la requête et renvoyer un résultat partiel, comme si les
  données d’entrée étaient épuisées.

<div id="group_by_overflow_mode_settings">
  ## Paramètres de `group_by_overflow_mode`
</div>

Le paramètre `group_by_overflow_mode` accepte également
la valeur `any` :

* `any` : poursuivre l’agrégation pour les clés déjà présentes dans l’ensemble, sans
  ajouter de nouvelles clés à l’ensemble.

<div id="relevant-settings">
  ## Liste des paramètres
</div>

Les paramètres suivants sont utilisés pour appliquer des restrictions sur la complexité des requêtes.

:::note
Les restrictions portant sur la « quantité maximale de quelque chose » peuvent prendre la valeur `0`,
ce qui signifie qu’elles sont « sans restriction ».
:::

| Paramètre                                                                                                              | Brève description                                                                                                                                                                          |
| ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`max_memory_usage`](/fr/operations/settings/settings#max_memory_usage)                                                   | La quantité maximale de RAM à utiliser pour exécuter une requête sur un seul serveur.                                                                                                      |
| [`max_memory_usage_for_user`](/fr/operations/settings/settings#max_memory_usage_for_user)                                 | La quantité maximale de RAM à utiliser pour exécuter les requêtes d’un utilisateur sur un seul serveur.                                                                                    |
| [`max_rows_to_read`](/fr/operations/settings/settings#max_rows_to_read)                                                   | Le nombre maximal de lignes pouvant être lues à partir d’une table lors de l’exécution d’une requête.                                                                                      |
| [`max_bytes_to_read`](/fr/operations/settings/settings#max_bytes_to_read)                                                 | Le nombre maximal d’octets (de données non compressées) pouvant être lus à partir d’une table lors de l’exécution d’une requête.                                                           |
| [`read_overflow_mode_leaf`](/fr/operations/settings/settings#read_overflow_mode_leaf)                                     | Définit ce qui se passe lorsque le volume de données lues dépasse l’une des limites des nœuds feuilles                                                                                     |
| [`max_rows_to_read_leaf`](/fr/operations/settings/settings#max_rows_to_read_leaf)                                         | Le nombre maximal de lignes pouvant être lues depuis une table locale sur un nœud feuille lors de l’exécution d’une requête distribuée                                                     |
| [`max_bytes_to_read_leaf`](/fr/operations/settings/settings#max_bytes_to_read_leaf)                                       | Le nombre maximal d’octets (de données non compressées) pouvant être lus depuis une table locale sur un nœud feuille lors de l’exécution d’une requête distribuée.                         |
| [`read_overflow_mode_leaf`](/fr/docs/operations/settings/settings#read_overflow_mode_leaf)                                | Définit ce qui se passe lorsque le volume de données lues dépasse l’une des limites des nœuds feuilles.                                                                                    |
| [`max_rows_to_group_by`](/fr/operations/settings/settings#max_rows_to_group_by)                                           | Le nombre maximal de clés uniques renvoyées par l’agrégation.                                                                                                                              |
| [`group_by_overflow_mode`](/fr/operations/settings/settings#group_by_overflow_mode)                                       | Définit ce qui se passe lorsque le nombre de clés uniques pour l’agrégation dépasse la limite                                                                                              |
| [`max_bytes_before_external_group_by`](/fr/operations/settings/settings#max_bytes_before_external_group_by)               | Active ou désactive l’exécution des clauses `GROUP BY` en mémoire externe.                                                                                                                 |
| [`max_bytes_ratio_before_external_group_by`](/fr/operations/settings/settings#max_bytes_ratio_before_external_group_by)   | La part de mémoire disponible autorisée pour `GROUP BY`. Une fois ce seuil atteint, la mémoire externe est utilisée pour l’agrégation.                                                     |
| [`max_bytes_before_external_sort`](/fr/operations/settings/settings#max_bytes_before_external_sort)                       | Active ou désactive l’exécution des clauses `ORDER BY` en mémoire externe.                                                                                                                 |
| [`max_bytes_ratio_before_external_sort`](/fr/operations/settings/settings#max_bytes_ratio_before_external_sort)           | La part de mémoire disponible autorisée pour `ORDER BY`. Une fois ce seuil atteint, le tri externe est utilisé.                                                                            |
| [`max_rows_to_sort`](/fr/operations/settings/settings#max_rows_to_sort)                                                   | Le nombre maximal de lignes avant le tri. Permet de limiter la consommation de mémoire lors du tri.                                                                                        |
| [`max_bytes_to_sort`](/fr/operations/settings/settings#max_rows_to_sort)                                                  | Le nombre maximal d’octets avant le tri.                                                                                                                                                   |
| [`sort_overflow_mode`](/fr/operations/settings/settings#sort_overflow_mode)                                               | Définit ce qui se passe si le nombre de lignes reçues avant le tri dépasse l’une des limites.                                                                                              |
| [`max_result_rows`](/fr/operations/settings/settings#max_result_rows)                                                     | Limite le nombre de lignes dans le résultat.                                                                                                                                               |
| [`max_result_bytes`](/fr/operations/settings/settings#max_result_bytes)                                                   | Limite la taille du résultat en octets (non compressés)                                                                                                                                    |
| [`result_overflow_mode`](/fr/operations/settings/settings#result_overflow_mode)                                           | Définit quoi faire si le volume du résultat dépasse l’une des limites.                                                                                                                     |
| [`max_execution_time`](/fr/operations/settings/settings#max_execution_time)                                               | Le temps d’exécution maximal de la requête en secondes.                                                                                                                                    |
| [`timeout_overflow_mode`](/fr/operations/settings/settings#timeout_overflow_mode)                                         | Définit quoi faire si la requête s’exécute plus longtemps que `max_execution_time` ou si le temps d’exécution estimé dépasse `max_estimated_execution_time`.                               |
| [`max_execution_time_leaf`](/fr/operations/settings/settings#max_execution_time_leaf)                                     | Semblable, sur le plan sémantique, à `max_execution_time`, mais appliqué uniquement aux nœuds feuilles pour les requêtes distribuées ou distantes.                                         |
| [`timeout_overflow_mode_leaf`](/fr/operations/settings/settings#timeout_overflow_mode_leaf)                               | Définit ce qui se passe lorsque la requête sur un nœud feuille s’exécute plus longtemps que `max_execution_time_leaf`.                                                                     |
| [`min_execution_speed`](/fr/operations/settings/settings#min_execution_speed)                                             | Vitesse d’exécution minimale en lignes par seconde.                                                                                                                                        |
| [`min_execution_speed_bytes`](/fr/operations/settings/settings#min_execution_speed_bytes)                                 | Le nombre minimal d’octets traités par seconde.                                                                                                                                            |
| [`max_execution_speed`](/fr/operations/settings/settings#max_execution_speed)                                             | Le nombre maximal de lignes traitées par seconde.                                                                                                                                          |
| [`max_execution_speed_bytes`](/fr/operations/settings/settings#max_execution_speed_bytes)                                 | Le nombre maximal d’octets traités par seconde.                                                                                                                                            |
| [`timeout_before_checking_execution_speed`](/fr/operations/settings/settings#timeout_before_checking_execution_speed)     | Vérifie, une fois le délai spécifié en secondes écoulé, que la vitesse d’exécution n’est pas trop lente (c’est-à-dire qu’elle n’est pas inférieure à `min_execution_speed`).               |
| [`max_estimated_execution_time`](/fr/operations/settings/settings#max_estimated_execution_time)                           | Le temps d’exécution estimé maximal de la requête en secondes.                                                                                                                             |
| [`max_columns_to_read`](/fr/operations/settings/settings#max_columns_to_read)                                             | Le nombre maximal de colonnes pouvant être lues depuis une table dans une seule requête.                                                                                                   |
| [`max_temporary_columns`](/fr/operations/settings/settings#max_temporary_columns)                                         | Le nombre maximal de colonnes temporaires qui doivent être conservées simultanément en RAM lors de l’exécution d’une requête, y compris les colonnes constantes.                           |
| [`max_temporary_non_const_columns`](/fr/operations/settings/settings#max_temporary_non_const_columns)                     | Le nombre maximal de colonnes temporaires qui doivent être conservées simultanément en RAM lors de l’exécution d’une requête, sans compter les colonnes constantes.                        |
| [`max_subquery_depth`](/fr/operations/settings/settings#max_subquery_depth)                                               | Définit ce qui se passe si une requête comporte plus de sous-requêtes imbriquées que le nombre spécifié.                                                                                   |
| [`max_ast_depth`](/fr/operations/settings/settings#max_ast_depth)                                                         | La profondeur d’imbrication maximale de l’arbre syntaxique d’une requête.                                                                                                                  |
| [`max_ast_elements`](/fr/operations/settings/settings#max_ast_elements)                                                   | Le nombre maximal d’éléments dans l’arbre syntaxique d’une requête.                                                                                                                        |
| [`max_rows_in_set`](/fr/operations/settings/settings#max_rows_in_set)                                                     | Le nombre maximal de lignes pour un ensemble de données dans la clause IN créée à partir d’une sous-requête.                                                                               |
| [`max_bytes_in_set`](/fr/operations/settings/settings#max_bytes_in_set)                                                   | Le nombre maximal d’octets (de données non compressées) utilisé par un ensemble dans la clause IN créée à partir d’une sous-requête.                                                       |
| [`set_overflow_mode`](/fr/operations/settings/settings#max_bytes_in_set)                                                  | Définit ce qui se passe lorsque la quantité de données dépasse l’une des limites.                                                                                                          |
| [`max_rows_in_distinct`](/fr/operations/settings/settings#max_rows_in_distinct)                                           | Le nombre maximal de lignes distinctes lors de l’utilisation de DISTINCT.                                                                                                                  |
| [`max_bytes_in_distinct`](/fr/operations/settings/settings#max_bytes_in_distinct)                                         | Le nombre maximal d’octets de l’état en mémoire (en octets non compressés), utilisé par une table de hachage lors de l’utilisation de DISTINCT.                                            |
| [`distinct_overflow_mode`](/fr/operations/settings/settings#distinct_overflow_mode)                                       | Définit ce qui se passe lorsque la quantité de données dépasse l’une des limites.                                                                                                          |
| [`max_rows_to_transfer`](/fr/operations/settings/settings#max_rows_to_transfer)                                           | Taille maximale (en lignes) pouvant être transmise à un serveur distant ou enregistrée dans une table temporaire lors de l’exécution de la section GLOBAL IN/JOIN.                         |
| [`max_bytes_to_transfer`](/fr/operations/settings/settings#max_bytes_to_transfer)                                         | Le nombre maximal d’octets (données non compressées) pouvant être transmis à un serveur distant ou enregistrés dans une table temporaire lors de l’exécution de la section GLOBAL IN/JOIN. |
| [`transfer_overflow_mode`](/fr/operations/settings/settings#transfer_overflow_mode)                                       | Définit ce qui se passe lorsque la quantité de données dépasse l’une des limites.                                                                                                          |
| [`max_rows_in_join`](/fr/operations/settings/settings#max_rows_in_join)                                                   | Limite le nombre de lignes dans la table de hachage utilisée lors de la jointure de tables.                                                                                                |
| [`max_bytes_in_join`](/fr/operations/settings/settings#max_bytes_in_join)                                                 | La taille maximale, en octets, de la table de hachage utilisée lors de la jointure de tables.                                                                                              |
| [`join_overflow_mode`](/fr/operations/settings/settings#join_overflow_mode)                                               | Définit l’action effectuée par ClickHouse lorsque l’une des limites de jointure suivantes est atteinte.                                                                                    |
| [`max_partitions_per_insert_block`](/fr/operations/settings/settings#max_partitions_per_insert_block)                     | Limite le nombre maximal de partitions dans un seul bloc inséré, et une exception est levée si le bloc contient trop de partitions.                                                        |
| [`throw_on_max_partitions_per_insert_block`](/fr/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | Permet de contrôler le comportement lorsque `max_partitions_per_insert_block` est atteint.                                                                                                 |
| [`max_temporary_data_on_disk_size_for_user`](/fr/operations/settings/settings#throw_on_max_partitions_per_insert_block)   | La quantité maximale de données consommée, en octets, par les fichiers temporaires sur disque pour l’ensemble des requêtes utilisateur exécutées simultanément.                            |
| [`max_temporary_data_on_disk_size_for_query`](/fr/operations/settings/settings#max_temporary_data_on_disk_size_for_query) | La quantité maximale de données consommée, en octets, par les fichiers temporaires sur disque pour l’ensemble des requêtes exécutées simultanément.                                        |
| [`max_sessions_for_user`](/fr/operations/settings/settings#max_sessions_for_user)                                         | Nombre maximal de sessions simultanées par utilisateur authentifié sur le serveur ClickHouse.                                                                                              |
| [`max_partitions_to_read`](/fr/operations/settings/settings#max_partitions_to_read)                                       | Limite le nombre maximal de partitions pouvant être consultées dans une seule requête.                                                                                                     |

<div id="obsolete-settings">
  ## Paramètres obsolètes
</div>

:::note
Les paramètres suivants sont obsolètes
:::

<div id="max-pipeline-depth">
  ### max_pipeline_depth
</div>

Profondeur maximale du pipeline. Elle correspond au nombre de transformations que chaque
bloc de données subit lors du traitement des requêtes. Elle est comptabilisée à l&#39;échelle d&#39;un
seul serveur. Si la profondeur du pipeline est plus élevée, une exception est levée.