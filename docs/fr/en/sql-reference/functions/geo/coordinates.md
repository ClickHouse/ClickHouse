---
description: 'Documentation sur les coordonnées'
sidebar_label: 'Coordonnées géographiques'
slug: /sql-reference/functions/geo/coordinates
title: 'Fonctions de manipulation des coordonnées géographiques'
doc_type: 'reference'
---

<div id="greatcircledistance">
  ## greatCircleDistance
</div>

Calcule la distance entre deux points à la surface de la Terre à l’aide de [la formule orthodromique](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Paramètres d’entrée**

* `lon1Deg` — Longitude du premier point en degrés. Plage : `[-180°, 180°]`.
* `lat1Deg` — Latitude du premier point en degrés. Plage : `[-90°, 90°]`.
* `lon2Deg` — Longitude du deuxième point en degrés. Plage : `[-180°, 180°]`.
* `lat2Deg` — Latitude du deuxième point en degrés. Plage : `[-90°, 90°]`.

Les valeurs positives correspondent aux latitudes nord et aux longitudes est, tandis que les valeurs négatives correspondent aux latitudes sud et aux longitudes ouest.

**Valeur renvoyée**

La distance entre deux points à la surface de la Terre, en mètres.

Génère une exception lorsque les valeurs des paramètres d’entrée sont en dehors de cette plage.

**Exemple**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673) AS greatCircleDistance
```

```text
┌─greatCircleDistance─┐
│            14128352 │
└─────────────────────┘
```

<div id="geodistance">
  ## geoDistance
</div>

Semblable à `greatCircleDistance`, mais calcule la distance sur l’ellipsoïde WGS-84 plutôt que sur une sphère. Cela fournit une approximation plus précise du géoïde terrestre.
Les performances sont identiques à celles de `greatCircleDistance` (sans impact sur les performances). Il est recommandé d’utiliser `geoDistance` pour calculer les distances à la surface de la Terre.

Note technique : pour des points suffisamment proches, nous calculons la distance à l’aide d’une approximation plane, avec la métrique du plan tangent au point médian des coordonnées.

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Paramètres d’entrée**

* `lon1Deg` — Longitude du premier point en degrés. Plage : `[-180°, 180°]`.
* `lat1Deg` — Latitude du premier point en degrés. Plage : `[-90°, 90°]`.
* `lon2Deg` — Longitude du deuxième point en degrés. Plage : `[-180°, 180°]`.
* `lat2Deg` — Latitude du deuxième point en degrés. Plage : `[-90°, 90°]`.

Les valeurs positives correspondent aux latitudes nord et aux longitudes est, et les valeurs négatives aux latitudes sud et aux longitudes ouest.

**Valeur renvoyée**

La distance entre deux points à la surface de la Terre, en mètres.

Génère une exception lorsque les valeurs des paramètres d’entrée sont en dehors de la plage.

**Exemple**

```sql
SELECT geoDistance(38.8976, -77.0366, 39.9496, -75.1503) AS geoDistance
```

```text
┌─geoDistance─┐
│   212458.73 │
└─────────────┘
```

<div id="greatcircleangle">
  ## greatCircleAngle
</div>

Calcule l’angle central entre deux points à la surface de la Terre à l’aide de [la formule orthodromique](https://en.wikipedia.org/wiki/Great-circle_distance).

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Paramètres d’entrée**

* `lon1Deg` — Longitude du premier point, en degrés.
* `lat1Deg` — Latitude du premier point, en degrés.
* `lon2Deg` — Longitude du deuxième point, en degrés.
* `lat2Deg` — Latitude du deuxième point, en degrés.

**Valeur renvoyée**

L’angle central entre deux points, en degrés.

**Exemple**

```sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

```text
┌─arc─┐
│  45 │
└─────┘
```

<div id="geotoutm">
  ## geoToUTM
</div>

Convertit des coordonnées géographiques WGS84 `(longitude, latitude)` en coordonnées [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system).

L’UTM est un ensemble de 60 projections transverses de Mercator, chacune couvrant une zone longitudinale large de 6°, qui permettent de convertir des coordonnées géographiques en une grille plane exprimée en mètres. La zone est sélectionnée automatiquement à partir de la longitude, en appliquant les exceptions standard pour la Norvège et le Svalbard, sauf si une `zone` explicite est fournie. L’UTM n’est défini que pour les latitudes comprises dans l’intervalle `[-80°, 84°]` ; pour les calottes polaires, on utilise le système UPS distinct.

```sql
geoToUTM(longitude, latitude[, zone])
```

**Arguments**

* `longitude` — Longitude en degrés. Plage : `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — Latitude en degrés. Plage : `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `zone` — Facultatif. Force la projection vers cette zone UTM au lieu de la sélectionner automatiquement. Plage : `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).

**Valeur renvoyée**

Un tuple nommé `(easting, northing, zone, band)` : `easting` et `northing` en mètres ([`Float64`](../../data-types/float.md)), le numéro de `zone` UTM ([`UInt8`](../../data-types/int-uint.md)) et la lettre de bande de latitude MGRS `band` ([`FixedString(1)`](../../data-types/fixedstring.md)). Une valeur `band` égale à `'N'` ou supérieure indique l&#39;hémisphère nord.

Génère une exception lorsque la latitude est hors de `[-80°, 84°]` ou que la longitude est hors de `[-180°, 180°]`.

**Exemple**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

<div id="utmtogeo">
  ## UTMToGeo
</div>

Convertit des coordonnées [UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) en coordonnées géographiques WGS84 `(longitude, latitude)`. Il s’agit de l’opération inverse de [`geoToUTM`](#geotoutm).

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**Arguments**

* `easting` — Coordonnée d’est en mètres (inclut la fausse abscisse de 500000 m). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `northing` — Coordonnée nord en mètres (inclut la fausse ordonnée de 10000000 m dans l’hémisphère sud). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `zone` — Numéro de zone UTM. Plage : `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).
* `is_north` — Hémisphère : `1` pour l’hémisphère nord, `0` pour l’hémisphère sud. [`(U)Int*`](../../data-types/int-uint.md).

**Valeur renvoyée**

Un tuple nommé `(longitude, latitude)` en degrés. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Exemple**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

<div id="geotomgrs">
  ## geoToMGRS
</div>

Encode les coordonnées géographiques WGS84 `(longitude, latitude)` en chaîne [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System).

La chaîne a la forme `<zone><band><100km square><easting><northing>`, par exemple `31UDQ4825111935`. L&#39;argument `precision` contrôle le nombre de chiffres utilisés pour chacune des valeurs d&#39;abscisse et d&#39;ordonnée : `5` (par défaut) pour 1 m, `4` pour 10 m, `3` pour 100 m, `2` pour 1 km, `1` pour 10 km et `0` pour la maille carrée de 100 km uniquement. MGRS n&#39;est défini que pour les latitudes comprises dans l&#39;intervalle `[-80°, 84°]`.

```sql
geoToMGRS(longitude, latitude[, precision])
```

**Arguments**

* `longitude` — Longitude en degrés. Plage : `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — Latitude en degrés. Plage : `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `precision` — Facultatif. Nombre de chiffres pour chacune des composantes est et nord. Valeur par défaut : `5`. Plage : `[0, 5]`. [`(U)Int*`](../../data-types/int-uint.md).

**Valeur renvoyée**

La chaîne de référence MGRS. [`String`](../../data-types/string.md).

**Exemple**

```sql
SELECT geoToMGRS(2.294497, 48.858222) AS mgrs, geoToMGRS(2.294497, 48.858222, 3) AS mgrs_100m;
```

```text
┌─mgrs────────────┬─mgrs_100m───┐
│ 31UDQ4825111935 │ 31UDQ482119 │
└─────────────────┴─────────────┘
```

<div id="mgrstogeo">
  ## MGRSToGeo
</div>

Décode une chaîne [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) en coordonnées géographiques WGS84 `(longitude, latitude)`. Il s’agit de l’inverse de [`geoToMGRS`](#geotomgrs).

Le point renvoyé est le centre de la maille de grille indiquée ; la précision du résultat correspond donc à celle encodée dans la chaîne. Les espaces dans l’entrée sont ignorés et la casse des lettres n’est pas prise en compte.

```sql
MGRSToGeo(mgrs)
```

**Arguments**

* `mgrs` — chaîne de référence MGRS à décoder. [`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md).

**Valeur renvoyée**

Un tuple nommé `(longitude, latitude)` en degrés. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**Exemple**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

<div id="pointinellipses">
  ## pointInEllipses
</div>

Vérifie si le point appartient à au moins une ellipse.
Les coordonnées sont exprimées dans un système de coordonnées cartésien.

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Paramètres d&#39;entrée**

* `x, y` — Coordonnées d&#39;un point dans le plan.
* `xᵢ, yᵢ` — Coordonnées du centre de la `i`-ème ellipse.
* `aᵢ, bᵢ` — Axes de la `i`-ème ellipse, en unités des coordonnées x et y.

Le nombre de paramètres d&#39;entrée doit être `2+4⋅n`, où `n` est le nombre d&#39;ellipses.

**Valeurs renvoyées**

`1` si le point se trouve à l&#39;intérieur d&#39;au moins une ellipse ; `0` dans le cas contraire.

**Exemple**

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

<div id="pointinpolygon">
  ## pointInPolygon
</div>

Indique si le point appartient au polygone dans le plan.

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Valeurs d’entrée**

* `(x, y)` — Coordonnées d’un point dans le plan. Type de données — [Tuple](../../data-types/tuple.md) — un tuple de deux nombres.
* `[(a, b), (c, d) ...]` — Sommets du polygone. Type de données — [Array](../../data-types/array.md). Chaque sommet est représenté par une paire de coordonnées `(a, b)`. Les sommets doivent être indiqués dans le sens horaire ou antihoraire. Le nombre minimal de sommets est de 3. Le polygone doit être une constante.
* La fonction prend également en charge les polygones avec des trous (découpes intérieures). Type de données — [Polygon](../../data-types/geo.md/#polygon). Passez soit le `Polygon` complet comme deuxième argument, soit d’abord l’anneau extérieur, puis chaque trou comme argument supplémentaire distinct.
* La fonction prend également en charge les multipolygones. Type de données — [MultiPolygon](../../data-types/geo.md/#multipolygon). Passez soit le `MultiPolygon` complet comme deuxième argument, soit chaque polygone qui le compose comme argument distinct.

**Valeurs renvoyées**

`1` si le point est à l’intérieur du polygone, `0` sinon.
Si le point se trouve sur le bord du polygone, la fonction peut renvoyer 0 ou 1.

**Exemple**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

> **Note**
> • Vous pouvez définir `validate_polygons = 0` pour contourner la validation géométrique.
> • `pointInPolygon` suppose que chaque polygone est bien formé. Si l’entrée s’auto-intersecte, comporte des anneaux mal ordonnés ou des arêtes qui se chevauchent, les résultats deviennent peu fiables, en particulier pour les points situés exactement sur une arête, un sommet ou à l’intérieur d’une auto-intersection où la notion de &quot;à l’intérieur&quot; ou &quot;à l’extérieur&quot; est indéfinie.
> • Lorsque l’argument polygone est constant et que le point est exprimé à l’aide de colonnes de clé indexées (par exemple, `pointInPolygon((x, y), constant_polygon)` sur une table où `x, y` font partie de la `PRIMARY KEY` ou sont couverts par un index `minmax`), ClickHouse peut utiliser à la fois la clé primaire et les index de saut de données `minmax` pour écarter les granules non pertinentes.