---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "M\xE9moire"
---

# Mémoire {#memory}

Le moteur de mémoire stocke les données en RAM, sous forme non compressée. Les données sont stockées exactement sous la même forme qu'elles sont reçues lors de la lecture. En d'autres termes, la lecture de ce tableau est entièrement gratuit.
L'accès aux données simultanées est synchronisé. Les verrous sont courts: les opérations de lecture et d'écriture ne se bloquent pas.
Les index ne sont pas pris en charge. La lecture est parallélisée.
La productivité maximale (plus de 10 Go / s) est atteinte sur les requêtes simples, car il n'y a pas de lecture à partir du disque, de décompression ou de désérialisation des données. (Il convient de noter que dans de nombreux cas, la productivité du moteur MergeTree est presque aussi élevée.)
Lors du redémarrage d'un serveur, les données disparaissent de la table et la table devient vide.
Normalement, l'utilisation de ce moteur de table n'est pas justifiée. Cependant, il peut être utilisé pour des tests, et pour des tâches où la vitesse maximale est requise sur un nombre relativement faible de lignes (jusqu'à environ 100 000 000).

Le moteur de mémoire est utilisé par le système pour les tables temporaires avec des données de requête externes (voir la section “External data for processing a query”), et pour la mise en œuvre globale dans (voir la section “IN operators”).

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/memory/) <!--hide-->
