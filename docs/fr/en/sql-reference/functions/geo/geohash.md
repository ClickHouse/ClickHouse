---
description: 'Documentation relative à Geohash'
sidebar_label: 'Geohash'
slug: /sql-reference/functions/geo/geohash
title: 'Fonctions pour travailler avec Geohash'
doc_type: 'reference'
---

<div id="geohash">
  ## Geohash
</div>

[Geohash](https://en.wikipedia.org/wiki/Geohash) est un système de géocodage qui subdivise la surface de la Terre en cellules de grille et encode chaque cellule en une courte chaîne de lettres et de chiffres. Il s’agit d’une structure de données hiérarchique : plus la chaîne geohash est longue, plus la position géographique est précise.

Si vous devez convertir manuellement des coordonnées géographiques en chaînes geohash, vous pouvez utiliser [geohash.org](http://geohash.co/)

<div id="geohashencode">
  ## geohashEncode
</div>

Encode la latitude et la longitude sous forme de chaîne [geohash](#geohash).

**Syntaxe**

```sql
geohashEncode(longitude, latitude, [precision])
```

**Valeurs d’entrée**

* `longitude` — Composante longitude de la coordonnée à encoder. Valeur flottante dans l’intervalle `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude` — Composante latitude de la coordonnée à encoder. Valeur flottante dans l’intervalle `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` (facultatif) — Longueur de la chaîne encodée obtenue. La valeur par défaut est `12`. Entier dans l’intervalle `[1, 12]`. [Int8](../../data-types/int-uint.md).

:::note

* Tous les paramètres de coordonnées doivent être du même type : soit `Float32`, soit `Float64`.
* Pour le paramètre `precision`, toute valeur inférieure à `1` ou supérieure à `12` est convertie silencieusement en `12`.
  :::

**Valeurs renvoyées**

* Chaîne alphanumérique représentant la coordonnée encodée (une version modifiée de l’alphabet d’encodage base32 est utilisée). [String](../../data-types/string.md).

**Exemple**

```sql title="Query"
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res;
```

```text title="Response"
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

<div id="geohashdecode">
  ## geohashDecode
</div>

Décode toute chaîne encodée en [geohash](#geohash) en longitude et en latitude.

**Syntaxe**

```sql
geohashDecode(hash_str)
```

**Valeurs d’entrée**

* `hash_str` — Chaîne encodée au format geohash.

**Valeurs renvoyées**

* Tuple `(longitude, latitude)` de valeurs `Float64` correspondant à la longitude et à la latitude. [Tuple](../../data-types/tuple.md)([Float64](../../data-types/float.md))

**Exemple**

```sql
SELECT geohashDecode('ezs42') AS res;
```

```text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

<div id="geohashesinbox">
  ## geohashesInBox
</div>

Renvoie un tableau de chaînes encodées en [geohash](#geohash), avec la précision donnée, situées à l&#39;intérieur du rectangle spécifié et dont les limites l&#39;intersectent ; il s&#39;agit essentiellement d&#39;une grille 2D aplatie en tableau.

**Syntaxe**

```sql
geohashesInBox(longitude_min, latitude_min, longitude_max, latitude_max, precision)
```

**Arguments**

* `longitude_min` — Longitude minimale. Plage : `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_min` — Latitude minimale. Plage : `[-90°, 90°]`. [Float](../../data-types/float.md).
* `longitude_max` — Longitude maximale. Plage : `[-180°, 180°]`. [Float](../../data-types/float.md).
* `latitude_max` — Latitude maximale. Plage : `[-90°, 90°]`. [Float](../../data-types/float.md).
* `precision` — Précision du geohash. Plage : `[1, 12]`. [UInt8](../../data-types/int-uint.md).

:::note
Tous les paramètres de coordonnées doivent être du même type : `Float32` ou `Float64`.
:::

**Valeurs renvoyées**

* Array de chaînes de longueur `precision` représentant des boîtes de geohash couvrant la zone fournie ; vous ne devez pas vous fier à l’ordre des éléments. [Array](../../data-types/array.md)([String](../../data-types/string.md)).
* `[]` - Array vide si les valeurs minimales de latitude et de longitude ne sont pas inférieures aux valeurs maximales correspondantes.

:::note
La fonction lève une exception si l’Array résultant contient plus de 10&#39;000&#39;000 éléments.
:::

**Exemple**

```sql title="Query"
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos;
```

```text title="Response"
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```