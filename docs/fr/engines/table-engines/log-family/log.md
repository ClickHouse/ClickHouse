---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: Journal
---

# Journal {#log}

Moteur appartient à la famille de journal des moteurs. Voir les propriétés communes des moteurs de journal et leurs différences dans le [Famille De Moteurs En Rondins](index.md) article.

Journal diffère de [TinyLog](tinylog.md) dans un petit fichier de “marks” réside avec les fichiers de colonne. Ces marques sont écrites sur chaque bloc de données et contiennent des décalages qui indiquent où commencer à lire le fichier afin d'ignorer le nombre de lignes spécifié. Cela permet de lire les données de table dans plusieurs threads.
Pour l'accès aux données simultanées, les opérations de lecture peuvent être effectuées simultanément, tandis que les opérations d'écriture bloc lit et l'autre.
Le moteur de journal ne prend pas en charge les index. De même, si l'écriture dans une table a échoué, la table est cassée et la lecture de celle-ci renvoie une erreur. Le moteur de journal est approprié pour les données temporaires, les tables en écriture unique, et à des fins de test ou de démonstration.

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/log/) <!--hide-->
