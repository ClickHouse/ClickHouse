---
description: 'Le moteur Memory stocke les données en RAM, sous forme non compressée. Les données sont
  stockées exactement dans la forme sous laquelle elles sont reçues en lecture. En d''autres termes, la lecture
  de cette table est pratiquement gratuite.'
sidebar_label: 'Memory'
sidebar_position: 110
slug: /engines/table-engines/special/memory
title: 'Moteur de table Memory'
doc_type: 'reference'
---

:::note
Lors de l&#39;utilisation du moteur de table Memory sur ClickHouse Cloud, les données ne sont pas répliquées sur tous les nœuds (par conception). Pour garantir que toutes les requêtes sont acheminées vers le même nœud et que le moteur de table Memory fonctionne comme prévu, vous pouvez procéder de l&#39;une des manières suivantes :

* Exécuter toutes les opérations dans la même session
* Utiliser un client qui s&#39;appuie sur TCP ou sur l&#39;interface native (ce qui permet la prise en charge des connexions persistantes) comme [clickhouse-client](/fr/interfaces/client)
  :::

Le moteur Memory stocke les données en RAM, sous forme non compressée. Les données sont stockées exactement dans la forme sous laquelle elles sont reçues en lecture. En d&#39;autres termes, la lecture de cette table est pratiquement gratuite.
L&#39;accès concurrent aux données est synchronisé. Les verrous sont de courte durée : les opérations de lecture et d&#39;écriture ne se bloquent pas mutuellement.
Les index ne sont pas pris en charge. La lecture est parallélisée.

Le débit maximal (plus de 10 Go/s) est atteint sur des requêtes simples, car il n&#39;y a ni lecture sur le disque, ni décompression, ni désérialisation des données. (Il convient de noter que, dans de nombreux cas, les performances du moteur MergeTree sont presque aussi élevées.)
Lors du redémarrage du serveur, les données disparaissent de la table et celle-ci devient vide.
En règle générale, l&#39;utilisation de ce moteur de table ne se justifie pas. Il peut toutefois être utilisé pour des tests, ainsi que pour des tâches exigeant une vitesse maximale sur un nombre relativement limité de lignes (jusqu&#39;à environ 100 000 000).

Le moteur Memory est utilisé par le système pour les tables temporaires contenant des données de requête externes (voir la section &quot;Données externes pour le traitement d&#39;une requête&quot;), ainsi que pour implémenter `GLOBAL IN` (voir la section &quot;Opérateurs IN&quot;).

Des bornes supérieure et inférieure peuvent être définies pour limiter la taille d&#39;une table du moteur Memory, ce qui lui permet de fonctionner comme un tampon circulaire (voir [Paramètres du moteur](#engine-parameters)).

<div id="engine-parameters">
  ## Paramètres du moteur
</div>

* `min_bytes_to_keep` — Nombre minimal d’octets à conserver lorsque la taille de la table en mémoire est plafonnée.
  * Valeur par défaut : `0`
  * Nécessite `max_bytes_to_keep`
* `max_bytes_to_keep` — Nombre maximal d’octets à conserver dans la table en mémoire, où les lignes les plus anciennes sont supprimées à chaque insertion (c.-à-d. tampon circulaire). Ce nombre peut dépasser la limite indiquée si, lors de l’ajout d’un bloc volumineux, le plus ancien lot de lignes à supprimer fait passer la table sous la limite `min_bytes_to_keep`.
  * Valeur par défaut : `0`
* `min_rows_to_keep` — Nombre minimal de lignes à conserver lorsque la taille de la table en mémoire est plafonnée.
  * Valeur par défaut : `0`
  * Nécessite `max_rows_to_keep`
* `max_rows_to_keep` — Nombre maximal de lignes à conserver dans la table en mémoire, où les lignes les plus anciennes sont supprimées à chaque insertion (c.-à-d. tampon circulaire). Ce nombre peut dépasser la limite indiquée si, lors de l’ajout d’un bloc volumineux, le plus ancien lot de lignes à supprimer fait passer la table sous la limite `min_rows_to_keep`.
  * Valeur par défaut : `0`
* `compress` - Indique s’il faut compresser les données en mémoire.
  * Valeur par défaut : `false`

<div id="usage">
  ## Utilisation
</div>

**Initialiser les paramètres**

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Modifier des paramètres**

```sql
ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

**Remarque :** Les paramètres de limitation `bytes` et `rows` peuvent être définis simultanément. Toutefois, ce sont les limites les plus basses de `max` et `min` qui seront appliquées.

<div id="examples">
  ## Exemples
</div>

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_bytes_to_keep = 4096, max_bytes_to_keep = 16384;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 8'192 bytes

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 1'024 bytes

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 8'192 bytes

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 65'536 bytes

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```

de même, pour les lignes :

```sql
CREATE TABLE memory (i UInt32) ENGINE = Memory SETTINGS min_rows_to_keep = 4000, max_rows_to_keep = 10000;

/* 1. testing oldest block doesn't get deleted due to min-threshold - 3000 rows */
INSERT INTO memory SELECT * FROM numbers(0, 1600); -- 1'600 rows

/* 2. adding block that doesn't get deleted */
INSERT INTO memory SELECT * FROM numbers(1000, 100); -- 100 rows

/* 3. testing oldest block gets deleted - 9216 bytes - 1100 */
INSERT INTO memory SELECT * FROM numbers(9000, 1000); -- 1'000 rows

/* 4. checking a very large block overrides all */
INSERT INTO memory SELECT * FROM numbers(9000, 10000); -- 10'000 rows

SELECT total_bytes, total_rows FROM system.tables WHERE name = 'memory' AND database = currentDatabase();
```

```text
┌─total_bytes─┬─total_rows─┐
│       65536 │      10000 │
└─────────────┴────────────┘
```