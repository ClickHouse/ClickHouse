---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: TinyLog
---

# TinyLog {#tinylog}

Le moteur appartient à la famille de moteurs en rondins. Voir [Famille De Moteurs En Rondins](index.md) pour les propriétés communes des moteurs en rondins et leurs différences.

Ce moteur de table est généralement utilisé avec la méthode write-once: écrivez des données une fois, puis lisez-les autant de fois que nécessaire. Par exemple, vous pouvez utiliser `TinyLog`- tapez des tables pour les données intermédiaires qui sont traitées en petits lots. Notez que le stockage des données dans un grand nombre de petites tables est inefficace.

Les requêtes sont exécutées dans un flux unique. En d'autres termes, ce moteur est destiné à des tables relativement petites (jusqu'à environ 1 000 000 de lignes). Il est logique d'utiliser ce moteur de table si vous avez beaucoup de petites tables, car il est plus simple que le [Journal](log.md) moteur (moins de fichiers doivent être ouverts).

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/tinylog/) <!--hide-->
