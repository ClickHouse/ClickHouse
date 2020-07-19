---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "D\xE9finir"
---

# Définir {#set}

Un ensemble de données qui est toujours en RAM. Il est conçu pour être utilisé sur le côté droit de l'opérateur (voir la section “IN operators”).

Vous pouvez utiliser INSERT pour insérer des données dans la table. De nouveaux éléments seront ajoutés à l'ensemble de données, tandis que les doublons seront ignorés.
Mais vous ne pouvez pas effectuer SELECT à partir de la table. La seule façon de récupérer des données est en l'utilisant dans la moitié droite de l'opérateur.

Les données sont toujours situées dans la RAM. Pour INSERT, les blocs de données insérées sont également écrits dans le répertoire des tables sur le disque. Lors du démarrage du serveur, ces données sont chargées dans la RAM. En d'autres termes, après le redémarrage, les données restent en place.

Pour un redémarrage brutal du serveur, le bloc de données sur le disque peut être perdu ou endommagé. Dans ce dernier cas, vous devrez peut-être supprimer manuellement le fichier contenant des données endommagées.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/set/) <!--hide-->
