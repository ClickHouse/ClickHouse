---
description: 'Documentation des fonctions de Geometry'
sidebar_label: 'Geometry'
slug: /sql-reference/functions/geo/geometry
title: 'Fonctions pour travailler avec Geometry'
doc_type: 'reference'
---

<div id="geometry">
  ## Geometry
</div>

Les fonctions Geometry permettent de calculer le périmètre et l’aire de types géométriques tels que POLYGON, LINESTRING, MULTIPOLYGON, MULTILINESTRING, RING et POINT. Utilisez des géométries avec le type Geometry. Si la valeur d’entrée est `NULL`, toutes les fonctions ci-dessous renverront 0.

<div id="perimetercartesian">
  ## perimeterCartesian
</div>

Calcule le périmètre de l’objet Geometry indiqué dans le système de coordonnées cartésien (plan).

**Syntaxe**

```sql
perimeterCartesian(geom)
```

**Arguments**

* `geom` — Objet de type Geometry. [Geometry](../../data-types/geo.md).

**Valeurs renvoyées**

* Nombre — Périmètre de l’objet, exprimé dans les unités du système de coordonnées. [Float64](../../data-types/float.md).

**Exemple**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT perimeterCartesian(geom) FROM geo_dst;
```

```response title="Response"
┌─perimeterCartesian(geom)─┐
│ 4.0                      │
└──────────────────────────┘
```

<div id="areacartesian">
  ## areaCartesian
</div>

Calcule l’aire de l’objet Geometry spécifié dans le système de coordonnées cartésien.

**Syntaxe**

```sql
areaCartesian(geom)
```

**Arguments**

* `geom` — objet de type Geometry. [Geometry](../../data-types/geo.md).

**Valeurs renvoyées**

* Nombre — Aire de l’objet, exprimée dans les unités du système de coordonnées. [Float64](../../data-types/float.md).

**Exemple**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT areaCartesian(geom) FROM geo_dst;
```

```response title="Response"
┌─areaCartesian(geom)─┐
│ -1                  │
└─────────────────────┘
```

<div id="perimeterspherical">
  ## perimeterSpherical
</div>

Calcule le périmètre d’un objet Geometry à la surface d’une sphère.

**Syntaxe**

```sql
perimeterSpherical(geom)
```

**Arguments**

* `geom` — objet de type Geometry. [Geometry](../../data-types/geo.md).

**Valeurs renvoyées**

* Nombre — Périmètre. [Float64](../../data-types/float.md).

**Exemple**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('LINESTRING(0 0,1 0,1 1,0 1,0 0)');
SELECT perimeterSpherical(geom) FROM geo_dst;
```

```response title="Response"
┌─perimeterSpherical(geom)─┐
│ 0                        │
└──────────────────────────┘
```

<div id="areaspherical">
  ## areaSpherical
</div>

Calcule l’aire d’un objet Geometry à la surface d’une sphère.

**Syntaxe**

```sql
areaSpherical(geom)
```

**Arguments**

* `geom` — Geometry. [Geometry](../../data-types/geo.md).

**Valeurs renvoyées**

* Nombre — Aire. [Float64](../../data-types/float.md).

**Exemple**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT areaSpherical(geom) FROM geo_dst;
```

```response title="Response"
┌─areaSpherical(geom)────┐
│ -0.0003046096848622019 │
└────────────────────────┘
```