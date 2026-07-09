---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'layout cache d’un dictionnaire'
sidebar_label: 'cache'
sidebar_position: 6
description: 'Stocke un dictionnaire dans un cache en mémoire de taille fixe.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Le type de disposition de dictionnaire `cached` stocke le dictionnaire dans un cache qui contient un nombre fixe de cellules.
Ces cellules contiennent les éléments fréquemment utilisés.

La clé du dictionnaire est de type [UInt64](/fr/sql-reference/data-types/int-uint.md).

Lors de la recherche dans un dictionnaire, le cache est consulté en premier. Pour chaque block de données, toutes les clés absentes du cache ou obsolètes sont demandées à la source à l’aide de `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Les données reçues sont ensuite écrites dans le cache.

Si des clés ne sont pas trouvées dans le dictionnaire, une tâche de mise à jour du cache est créée et ajoutée à la file d’attente de mise à jour. Les propriétés de la file d’attente de mise à jour peuvent être contrôlées avec les settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

Pour les dictionnaires de cache, la [lifetime](../lifetime.md) d’expiration des données dans le cache peut être définie. Si une durée supérieure à `lifetime` s’est écoulée depuis le loading des données dans une cellule, la valeur de la cellule n’est pas utilisée et la clé expire. La clé est de nouveau demandée la prochaine fois qu’elle doit être utilisée. Ce comportement peut être configuré avec le setting `allow_read_expired_keys`.

C’est la méthode la moins efficace de toutes pour stocker des dictionnaires. Les performances du cache dépendent fortement de settings corrects et du scénario d’utilisation. Un dictionnaire de type cache n’est performant que lorsque les taux de réussite sont suffisamment élevés (99 % et plus recommandés). Vous pouvez consulter le taux de réussite moyen dans la table [system.dictionaries](/fr/operations/system-tables/dictionaries.md).

Si le setting `allow_read_expired_keys` est défini sur 1 (0 par défaut), le dictionnaire peut alors prendre en charge les mises à jour asynchrones. Si un client demande des clés et qu’elles sont toutes dans le cache, mais que certaines sont expirées, le dictionnaire renvoie les clés expirées au client et les redemande à la source de manière asynchrone.

Pour améliorer les performances du cache, utilisez une subquery avec `LIMIT` et appelez la fonction avec le dictionnaire à l’extérieur.

Tous les types de sources sont pris en charge.

Exemple de settings :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <layout>
        <cache>
            <!-- La taille du cache, en nombre de cellules. Arrondie à la puissance de deux supérieure. -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- Permet de lire les clés expirées. -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- Taille maximale de la file d'attente de mise à jour. -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- Délai d'attente maximal en millisecondes pour placer une tâche de mise à jour dans la file d'attente. -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- Délai d'attente maximal en millisecondes pour qu'une tâche de mise à jour se termine. -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- Nombre maximal de threads pour la mise à jour du dictionnaire de cache. -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Définissez une taille de cache suffisamment grande. Vous devrez faire des essais pour déterminer le nombre de cellules :

1. Définissez une valeur.
2. Exécutez des queries jusqu’à ce que le cache soit complètement rempli.
3. Évaluez la consommation mémoire à l’aide de la table `system.dictionaries`.
4. Augmentez ou diminuez le nombre de cellules jusqu’à atteindre la consommation mémoire requise.

:::note
ClickHouse n’est pas recommandé comme source pour cette disposition. Les lookups de dictionnaire nécessitent des lectures ponctuelles aléatoires, ce qui ne correspond pas à l’access pattern pour lequel ClickHouse est optimisé.
:::