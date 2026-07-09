---
description: 'Documentation sur NumericIndexedVector et ses fonctions'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'Fonctions de NumericIndexedVector'
doc_type: 'reference'
---

NumericIndexedVector est une structure de données abstraite qui encapsule un vecteur et prend en charge des opérations d’agrégation de vecteurs ainsi que des opérations point à point. Bit-Sliced Index est sa méthode de stockage. Pour les fondements théoriques et les cas d’usage, consultez l’article [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).

<div id="bit-sliced-index">
  ## BSI
</div>

Dans la méthode de stockage BSI (Bit-Sliced Index), les données sont stockées sous forme de [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268), puis compressées à l’aide de [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap). Les opérations d’agrégation et les opérations point à point sont effectuées directement sur les données compressées, ce qui peut considérablement améliorer l’efficacité du stockage et des requêtes.

Un vecteur contient des indices et les valeurs qui leur correspondent. Voici quelques caractéristiques et contraintes de cette structure de données en mode de stockage BSI :

* Le type d’indice peut être `UInt8`, `UInt16` ou `UInt32`. **Remarque :** compte tenu des performances de l’implémentation 64 bits de Roaring Bitmap, le format BSI ne prend pas en charge `UInt64`/`Int64`.
* Le type de valeur peut être `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32` ou `Float64`. **Remarque :** le type de valeur ne s’élargit pas automatiquement. Par exemple, si vous utilisez `UInt8` comme type de valeur, toute somme qui dépasse la capacité de `UInt8` entraînera un overflow au lieu d’être promue vers un type supérieur ; de même, les opérations sur des entiers produiront des résultats entiers (par exemple, la division ne sera pas automatiquement convertie en résultat à virgule flottante). Il est donc important de planifier et de définir le type de valeur à l’avance. En pratique, les types à virgule flottante (`Float32`/`Float64`) sont couramment utilisés.
* Seuls deux vecteurs ayant le même type d’indice et le même type de valeur peuvent faire l’objet d’opérations.
* Le stockage sous-jacent utilise Bit-Sliced Index, les bitmaps servant à stocker les indices. Roaring Bitmap est utilisé comme implémentation concrète de bitmap. Il est recommandé de concentrer autant que possible les indices dans un petit nombre de conteneurs Roaring Bitmap afin de maximiser la compression et les performances des requêtes.
* Le mécanisme Bit-Sliced Index convertit les valeurs en binaire. Pour les types à virgule flottante, la conversion utilise une représentation en virgule fixe, ce qui peut entraîner une perte de précision. La précision peut être ajustée en personnalisant le nombre de bits utilisés pour la partie fractionnaire. La valeur par défaut est de 24 bits, ce qui est suffisant dans la plupart des cas. Vous pouvez personnaliser le nombre de bits de la partie entière et de la partie fractionnaire lors de la construction de NumericIndexedVector à l’aide de la fonction d’agrégation groupNumericIndexedVector avec `-State`.
* Il existe trois cas pour les indices : valeur non nulle, valeur nulle et absence de valeur. Dans NumericIndexedVector, seules les valeurs non nulles et nulles sont stockées. En outre, dans les opérations point à point entre deux NumericIndexedVectors, la valeur d’un indice inexistant est traitée comme 0. En cas de division, le résultat est zéro lorsque le diviseur est zéro.

<div id="create-numeric-indexed-vector-object">
  ## Créer un objet numericIndexedVector
</div>

Il existe deux façons de créer cette structure : l&#39;une consiste à utiliser la fonction d&#39;agrégation `groupNumericIndexedVector` avec `-State`.
Vous pouvez ajouter le suffixe `-if` pour prendre en charge une condition supplémentaire.
La fonction d&#39;agrégation ne traitera que les lignes qui remplissent la condition.
L&#39;autre consiste à la construire à partir d&#39;une map à l&#39;aide de `numericIndexedVectorBuild`.
La fonction `groupNumericIndexedVectorState` permet de personnaliser le nombre de bits pour la partie entière et la partie fractionnaire via des paramètres, tandis que `numericIndexedVectorBuild` ne le permet pas.

<div id="group-numeric-indexed-vector">
  ## groupNumericIndexedVector
</div>

Construit un NumericIndexedVector à partir de deux colonnes de données et renvoie la somme de toutes les valeurs au format `Float64`. Si le suffixe `State` est ajouté, la fonction renvoie un objet NumericIndexedVector.

**Syntaxe**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**Paramètres**

* `type` : String, facultatif. Indique le format de stockage. Actuellement, seul `'BSI'` est pris en charge.
* `integer_bit_num` : `UInt32`, facultatif. Applicable au format de stockage `'BSI'`, ce paramètre indique le nombre de bits utilisés pour la partie entière. Lorsque le type d’index est un type entier, la valeur par défaut correspond au nombre de bits utilisés pour stocker l’index. Par exemple, si le type d’index est UInt16, la valeur par défaut de `integer_bit_num` est 16. Pour les types d’index Float32 et Float64, la valeur par défaut de integer&#95;bit&#95;num est 40, de sorte que la partie entière des données représentables se situe dans la plage `[-2^39, 2^39 - 1]`. La plage autorisée est `[0, 64]`.
* `fraction_bit_num` : `UInt32`, facultatif. Applicable au format de stockage `'BSI'`, ce paramètre indique le nombre de bits utilisés pour la partie fractionnaire. Lorsque le type de valeur est un entier, la valeur par défaut est 0 ; lorsqu’il s’agit d’un type Float32 ou Float64, la valeur par défaut est 24. La plage valide est `[0, 24]`.
* Il existe également une contrainte selon laquelle la plage valide de integer&#95;bit&#95;num + fraction&#95;bit&#95;num est [0, 64].
* `col1` : La colonne d’index. Types pris en charge : `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`.
* `col2` : La colonne de valeur. Types pris en charge : `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**Valeur de retour**

Une valeur `Float64` représentant la somme de toutes les valeurs.

**Exemple**

Données de test :

```text
UserID  PlayTime
1       10
2       20
3       30
```

Requête &amp; résultat :

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

:::note
La documentation ci-dessous est générée à partir de la table système `system.functions`.
:::

{/* 
  les balises ci-dessous servent à générer la documentation à partir des tables système et ne doivent pas être supprimées.
  Pour plus de détails, voir https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }