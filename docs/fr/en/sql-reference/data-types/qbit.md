---
description: 'Documentation du type de données QBit dans ClickHouse, qui permet une quantification fine pour la recherche vectorielle approximative'
keywords: ['qbit', 'type de données']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'Type de données QBit'
doc_type: 'reference'
---

Le type de données `QBit` réorganise le stockage des vecteurs afin d’accélérer les recherches approximatives. Au lieu de stocker ensemble les éléments de chaque vecteur, il regroupe les mêmes positions de bits dans l’ensemble des vecteurs.
Ce format conserve les vecteurs en pleine précision tout en vous permettant de choisir le niveau de quantification fine au moment de la recherche : lisez moins de bits pour réduire les E/S et accélérer les calculs, ou davantage de bits pour une meilleure précision. Vous bénéficiez ainsi des gains de vitesse liés à la réduction des transferts de données et des calculs grâce à la quantification, tout en conservant l’accès aux données d’origine lorsque nécessaire.

Pour déclarer une colonne de type `QBit`, utilisez la syntaxe suivante :

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – le type de chaque élément du vecteur. Les types autorisés sont `Int8`, `BFloat16`, `Float32` et `Float64`
* `dimension` – le nombre d’éléments de chaque vecteur
* `stride` – facultatif. Le nombre de dimensions stockées ensemble dans un groupe de flux. S’il est omis, sa valeur par défaut est `dimension` (un seul groupe). S’il est renseigné, `dimension` doit être un multiple de `stride` et, si `stride` est inférieur à `dimension`, `stride` doit être un multiple de 8. Voir [Strides](#strides).

<div id="creating-qbit">
  ## Création de QBit
</div>

Utilisation du type `QBit` dans la définition d’une colonne de table :

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

<div id="converting-arrays-to-qbit">
  ## Conversion des tableaux en QBit
</div>

Les tableaux sont convertis en `QBit` lorsque leur longueur correspond à la dimension de `QBit`. Le type des éléments du tableau n’a pas besoin de correspondre à celui des éléments de `QBit`. Tout type d’élément numérique est automatiquement converti. Vous pouvez ainsi déplacer directement une colonne existante d’embeddings vers une colonne `QBit` :

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

La conversion fonctionne également de manière explicite avec `CAST`, par exemple `CAST(embedding AS QBit(Float32, 8))`.

<div id="converting-qbit-to-arrays">
  ## Conversion de QBit en tableaux
</div>

La conversion inverse reconstruit le vecteur d’origine à partir de la représentation transposée par bits ; ainsi, la conversion d’un `QBit` en `Array` renvoie les valeurs stockées. C’est l’inverse de [la conversion de tableaux en `QBit`](#converting-arrays-to-qbit) :

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

Le tableau reconstruit utilise le type d’élément de `QBit`, puis ses éléments sont convertis dans le type d’élément du tableau demandé. Un cast qui modifie aussi le type d’élément, par exemple de `QBit(Float32, N)` vers `Array(Float64)`, fonctionne donc également.

Un aller-retour `Array` -&gt; `QBit` -&gt; `Array` est sans perte pour `Int8`, `Float32` et `Float64`. Pour `BFloat16`, il équivaut à une conversion directe en `BFloat16` — la seule précision perdue est celle propre à `BFloat16`.

Lorsque la `dimension` n’est pas un multiple de 8, les éléments de remplissage en fin de tableau présents dans la représentation interne sont supprimés ; le résultat comporte donc toujours exactement `dimension` éléments.

<div id="qbit-subcolumns">
  ## Sous-colonnes QBit
</div>

`QBit` implémente un schéma d’accès par sous-colonnes qui permet d’accéder individuellement aux plans de bits des vecteurs stockés. Chaque position de bit est accessible à l’aide de la syntaxe `.N`, où `N` correspond à la position du bit :

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

Le nombre de sous-colonnes accessibles dépend du type d’élément (et, lorsqu’il y a des strides, du nombre de groupes de stride — voir [Strides](#strides)) :

* `Int8` : 8 sous-colonnes par groupe de stride (1-8)
* `BFloat16` : 16 sous-colonnes par groupe de stride (1-16)
* `Float32` : 32 sous-colonnes par groupe de stride (1-32)
* `Float64` : 64 sous-colonnes par groupe de stride (1-64)

<div id="strides">
  ## Strides
</div>

Par défaut, un `QBit` stocke chaque plan de bits dans un flux unique couvrant l’ensemble des `dimension` dimensions, de sorte qu’une recherche lit toujours des plans de bits complets sur tout le vecteur. Le paramètre facultatif `stride` partitionne les `dimension` dimensions en `dimension / stride` groupes contigus et stocke les plans de bits de chaque groupe dans des flux distincts. Cela permet à une recherche portant uniquement sur les `D` premières dimensions (avec `D` multiple de `stride`) de ne lire que les flux des groupes couvrant ces dimensions — ce qui est utile pour les [embeddings Matryoshka](https://arxiv.org/abs/2205.13147), où les premières dimensions forment un embedding exploitable de dimension inférieure.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

Ici, les 4096 dimensions sont réparties en 4 groupes de 1024. Les sous-colonnes suivent un ordre où le groupe prime : avec `BFloat16` (16 plans de bits), `vec.1` … `vec.16` correspondent aux 16 plans de bits du premier groupe de stride (dimensions 1–1024), `vec.17` … `vec.32` appartiennent au deuxième groupe (dimensions 1025–2048), et ainsi de suite. De manière générale, `vec.N` lit le plan de bits `(N-1) % element_size` du groupe de stride `(N-1) / element_size`.

Pour exécuter une recherche en dimension réduite, indiquez le nombre de dimensions à lire comme quatrième argument des fonctions de distance transposées (voir ci-dessous). Le vecteur de référence doit comporter exactement ce nombre d’éléments, et cette valeur doit être un multiple de `stride`.

<div id="vector-search-functions">
  ## Fonctions de recherche vectorielle
</div>

Voici les fonctions de distance pour la recherche de similarité vectorielle qui utilisent le type de données `QBit` :

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

Pour un `QBit` avec stride, ces fonctions acceptent un quatrième argument facultatif, `used_dims` — le nombre de premières dimensions à lire —, ce qui permet de lire uniquement les groupes de stride couvrant ces dimensions :

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```