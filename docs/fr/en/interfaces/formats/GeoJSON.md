---
alias: []
description: 'Format d’entrée et de sortie pour les documents GeoJSON de type FeatureCollection : en entrée, une ligne par objet avec des colonnes id, geometry et properties ; en sortie, un objet par ligne.'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Les données [GeoJSON](https://geojson.org/) sont échangées sous la forme d’un unique document [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3), que ClickHouse associe à trois colonnes — `id`, `geometry` et `properties` — avec un ensemble par `Feature`. La [lecture](#reading-data) d’un document produit une ligne par entité ; l’[écriture](#writing-data) produit une entité par ligne.

<div id="reading-data">
  ## Lecture des données
</div>

La lecture d’une `FeatureCollection` produit une ligne par feature, avec le schéma fixe suivant :

| Colonne      | Type               | Description                                                                                                                                                                                        |
| ------------ | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`         | `Nullable(String)` | Le membre `id` de la feature (une chaîne JSON ou un nombre), stocké sous forme de texte ; `NULL` si `id` est absent ou `null`, tandis qu’un id explicitement vide est conservé sous la forme `''`. |
| `geometry`   | `Geometry`         | La géométrie de la feature, stockée dans un type variant `Geometry`.                                                                                                                               |
| `properties` | `Nullable(JSON)`   | L’objet `properties` de la feature, stocké dans une colonne `JSON` semi-structurée. Une valeur explicite `"properties": null` est conservée comme `NULL`.                                          |

Chaque géométrie est stockée dans le type `Geometry` de ClickHouse (un `Variant`). Les types de géométrie GeoJSON pris en charge sont `Point`, `LineString`, `MultiLineString`, `Polygon` et `MultiPolygon`. Les deux autres types de géométrie GeoJSON, `GeometryCollection` et `MultiPoint`, ne peuvent pas être représentés par le type `Geometry` ; la lecture de l’un d’eux dans la colonne `geometry` déclenche par défaut une exception, mais ce comportement peut être modifié afin d’insérer `NULL` à la place — voir [Gestion des types de géométrie non pris en charge](#unsupported-geometry) ci-dessous. Par défaut, la colonne `geometry` vaut `NULL` uniquement lorsque la géométrie d’une feature est un `null` JSON explicite ; avec `input_format_geojson_unsupported_geometry_handling = 'null'`, elle vaut aussi `NULL` pour un type de géométrie non pris en charge.

La structure du document est validée : le `type` de niveau supérieur doit être `FeatureCollection` et chaque élément de `features` doit avoir `type` égal à `Feature`. Par défaut, les coordonnées doivent respecter les invariants de forme GeoJSON : une `LineString` (et chaque ligne d’une `MultiLineString`) doit comporter au moins deux points, et un anneau de `Polygon` (ainsi que chaque anneau d’un `MultiPolygon`) doit être fermé et comporter au moins quatre points (voir [Validation des géométries](#geometry-validation)). Les documents mal formés sont rejetés plutôt que chargés silencieusement.

L’ordre des clés est flexible : le `type` de niveau supérieur peut apparaître avant ou après le tableau `features`, et dans un objet géométrique, `coordinates` peut apparaître avant ou après `type`.

L’inférence du schéma renvoie le schéma fixe ci-dessus, de sorte que `DESCRIBE` et `SELECT ... FROM format(...)` fonctionnent sans définition de table.

Étant donné le fichier GeoJSON suivant `london.geojson`, qui contient un mélange de types de géométrie :

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [-0.0761, 51.5081]},
            "properties": {"name": "Tower of London", "feature_type": "landmark", "year_built": 1078}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {
                "type": "LineString",
                "coordinates": [[-0.2500, 51.4700], [-0.1800, 51.4900], [-0.1200, 51.5060], [-0.0700, 51.5050], [0.0000, 51.5100]]
            },
            "properties": {"name": "River Thames", "feature_type": "river", "length_km": 346}
        },
        {
            "type": "Feature",
            "id": "3",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-0.1880, 51.5074], [-0.1533, 51.5074], [-0.1533, 51.5153], [-0.1880, 51.5153], [-0.1880, 51.5074]]]
            },
            "properties": {"name": "Hyde Park", "feature_type": "park", "area_km2": 1.42}
        }
    ]
}
```

Nous pouvons interroger le fichier et examiner les types de géométrie :

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─name────────────┬─geo_type───┐
│ 1  │ Tower of London │ Point      │
│ 2  │ River Thames    │ LineString │
│ 3  │ Hyde Park       │ Polygon    │
└────┴─────────────────┴────────────┘
```

L’extension de fichier `.geojson` est détectée automatiquement ; l’argument de format peut donc être omis :

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

Nous pouvons utiliser `variantType` pour connaître le type sous-jacent de chaque objet Geometry :

```sql title="Query"
SELECT properties.name AS name, geometry, variantType(geometry)
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
geometry:              (-0.0761,51.5081)
variantType(geometry): Point

Row 2:
──────
name:                  River Thames
geometry:              [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
variantType(geometry): LineString

Row 3:
──────
name:                  Hyde Park
geometry:              [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
variantType(geometry): Polygon
```

Et nous pouvons extraire les données sous-jacentes ainsi :

```sql title="Query"
SELECT properties.name AS name, variantType(geometry), geometry.Point, geometry.LineString, geometry.Polygon
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
variantType(geometry): Point
geometry.Point:        (-0.0761,51.5081)
geometry.LineString:   []
geometry.Polygon:      []

Row 2:
──────
name:                  River Thames
variantType(geometry): LineString
geometry.Point:        (0,0)
geometry.LineString:   [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
geometry.Polygon:      []

Row 3:
──────
name:                  Hyde Park
variantType(geometry): Polygon
geometry.Point:        (0,0)
geometry.LineString:   []
geometry.Polygon:      [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
```

L’accès à une sous-colonne de `Geometry` renvoie la valeur lorsque la ligne contient ce type, et sinon la valeur par défaut du type — `(0,0)` pour `Point` et `[]` pour les types basés sur des tableaux — utilisez donc `variantType(geometry)` pour identifier celui qui est défini.

Nous pouvons également ingérer des données GeoJSON dans une table :

```sql title="Query"
CREATE TABLE london
(
    id           String,
    geometry     Geometry,
    properties   Nullable(JSON),
    name         String MATERIALIZED properties.name,
    feature_type String MATERIALIZED properties.feature_type
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO london
SELECT id, geometry, properties
FROM file('london.geojson', GeoJSON);
```

Interrogez ensuite selon le type d’entité :

```sql title="Query"
SELECT name, feature_type, variantType(geometry) AS geo_type
FROM london
ORDER BY id;
```

```response title="Response"
┌─name────────────┬─feature_type─┬─geo_type───┐
│ Tower of London │ landmark     │ Point      │
│ River Thames    │ river        │ LineString │
│ Hyde Park       │ park         │ Polygon    │
└─────────────────┴──────────────┴────────────┘
```

On peut également inférer le schéma des données GeoJSON sans définition de table :

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}');
```

```response title="Response"
┌─name───────┬─type─────────────┐
│ id         │ Nullable(String) │
│ geometry   │ Geometry         │
│ properties │ Nullable(JSON)   │
└────────────┴──────────────────┘
```

<div id="unsupported-geometry">
  ### Gestion des types de géométrie non pris en charge
</div>

Certains types de géométrie GeoJSON valides — tels que `GeometryCollection` et `MultiPoint` — ne peuvent pas être représentés par le type `Geometry` de ClickHouse. Vous pouvez contrôler ce qui se passe lorsqu&#39;une telle géométrie doit être stockée dans la colonne `geometry` à l&#39;aide du paramètre `input_format_geojson_unsupported_geometry_handling`. Les valeurs possibles sont :

* `'throw'` — lever une exception (par défaut)
* `'null'` — insérer une valeur `NULL` dans la colonne `geometry` et poursuivre l&#39;analyse

Cette gestion s&#39;applique uniquement lorsque la colonne `geometry` est lue. Lorsque `geometry` ne fait pas partie des colonnes de sortie demandées (par exemple `SELECT id FROM ...`), une géométrie non prise en charge est tout de même validée pour vérifier qu&#39;elle est bien formée, mais ne déclenche pas ce traitement — elle ne lève pas d&#39;exception et n&#39;insère pas non plus de `NULL`, car aucune valeur de géométrie n&#39;est matérialisée.

<div id="reading-limitations">
  ### Limites
</div>

À la lecture, seul ce qui correspond au schéma fixe est pris en compte ; certaines informations GeoJSON ne sont donc pas conservées :

* Seuls `id`, `geometry` et `properties` sont produits ; les autres éléments de la structure du document ne sont pas exposés sous forme de colonnes.
* La troisième coordonnée d’une position (l’altitude), ainsi que toutes les suivantes, sont supprimées — les positions deviennent `[longitude, latitude]`.
* `bbox` et les membres externes (par exemple un `name` ou un `crs` au niveau supérieur, ou des membres supplémentaires dans une `Feature`) sont ignorés.
* Un `id` numérique est stocké sous forme de texte, si bien que la distinction entre chaîne et nombre est perdue ; un `id` absent ou `null` devient `NULL`.
* `GeometryCollection` et `MultiPoint` ne peuvent pas être représentés — voir [Gestion des types de géométrie non pris en charge](#unsupported-geometry).

<div id="writing-data">
  ## Écriture des données
</div>

L’écriture d’un jeu de résultats produit une unique [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) GeoJSON, avec une `Feature` par ligne.

Les colonnes du résultat sont associées à chaque `Feature` comme suit :

| Membre de Feature | Construit à partir de                | Remarques                                                                                                                                                                                                                                                                                                                      |
| ----------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `type`            | —                                    | Toujours `"Feature"`.                                                                                                                                                                                                                                                                                                          |
| `geometry`        | l’unique colonne de type géométrique | Exactement une colonne de type géométrique est requise, sinon la requête est rejetée. Une géométrie `NULL` est écrite sous la forme `null`.                                                                                                                                                                                    |
| `id`              | une colonne nommée `id`              | Omis lorsque la valeur est `NULL`. Une colonne `String` est écrite comme une chaîne JSON, une colonne numérique comme un nombre JSON.                                                                                                                                                                                          |
| `properties`      | toutes les colonnes restantes        | Une colonne unique nommée `properties`, dont le type est de type objet (`JSON`, `Map` ou un `Tuple` nommé), est écrite directement comme objet `properties` au lieu d’être imbriquée sous une clé `properties`. Sinon, chaque colonne restante devient une propriété ayant son nom pour clé (objet vide s’il n’y en a aucune). |

La colonne de type géométrique peut être la variante `Geometry` ou un type géo spécifique ; chacune correspond à un type de géométrie GeoJSON :

| Type ClickHouse   | GeoJSON `"type"`                          |
| ----------------- | ----------------------------------------- |
| `Point`           | `Point`                                   |
| `LineString`      | `LineString`                              |
| `MultiLineString` | `MultiLineString`                         |
| `Polygon`         | `Polygon`                                 |
| `MultiPolygon`    | `MultiPolygon`                            |
| `Ring`            | `Polygon` (un seul anneau)                |
| `Geometry`        | le type de la variante active (ou `null`) |

`Ring` n’est pas un type de géométrie GeoJSON — un [anneau linéaire](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) est un composant d’un `Polygon` — donc une valeur `Ring` est écrite comme un `Polygon` à anneau unique.

<div id="writing-examples">
  ### Exemples
</div>

En reprenant la table `london` [créée plus haut](#reading-data), l’exportation de colonnes d’attribut simples transforme chaque colonne autre que `id` et `geometry` en propriété :

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

Comme une unique colonne de type objet nommée `properties` est écrite telle quelle, la lecture d’un fichier GeoJSON puis sa réécriture à l’identique reproduisent le document (les colonnes `id`, `geometry` et `properties` sont celles inférées pour le fichier) :

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

Une colonne numérique `id` est représentée comme un nombre JSON (un `id` `Nullable` dont la valeur est `NULL` est entièrement omis) :

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

Un `Ring` s’écrit sous la forme d’un `Polygon` à un seul anneau :

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### Écrire dans un fichier
</div>

Utilisez `INTO OUTFILE` pour écrire un fichier GeoJSON côté client :

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

Le serveur peut écrire lui-même le fichier grâce à la fonction de table `file` (l’extension `.geojson` sélectionne automatiquement le format) :

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="writing-limitations">
  ### Limitations
</div>

:::note
Les types geo de ClickHouse n’intègrent aucun système de référence de coordonnées ; la sortie suppose donc que les coordonnées sont déjà des longitudes/latitudes WGS84 dans l’ordre `[longitude, latitude]`, comme l’exige la [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4). Aucune reprojection ni permutation d’axes n’est effectuée ; par conséquent, des coordonnées projetées — ou des données stockées sous la forme `(latitude, longitude)` — produisent un GeoJSON structurellement valide mais non conforme.
:::

La sortie reflète uniquement ce que ClickHouse stocke :

* Les informations perdues à la lecture — l’élévation d’une position, `bbox`, les membres étrangers et la distinction entre chaîne et nombre pour un `id` — ne peuvent pas être restituées ; voir [Reading limitations](#reading-limitations).
* Les coordonnées sont écrites à partir de valeurs `Float64` en utilisant leur représentation aller-retour la plus courte.
* Un objet `properties` pris directement depuis une colonne `JSON` est émis selon l’ordre canonique des clés du type `JSON`, qui peut différer de l’entrée.

Les géométries sont écrites exactement telles qu’elles sont stockées — l’ordre des coordonnées et le winding sont préservés. Par défaut, la validité des formes GeoJSON est vérifiée à l’écriture (voir [Geometry validation](#geometry-validation)) : une géométrie qui n’est pas une forme GeoJSON valide, comme une `LineString` avec un seul point ou un anneau de `Polygon` non fermé, est rejetée afin que le document écrit puisse être relu correctement. Définissez `format_geojson_validate_geometry = 0` pour émettre ces géométries telles quelles, ce qui produit un GeoJSON structurellement valide mais non conforme. L’invariant de la règle de la main droite (winding) n’est appliqué dans aucun des cas, et la distinction entre `null` et un objet `properties` vide est préservée.

<div id="geometry-validation">
  ## Validation de la géométrie
</div>

Le paramètre `format_geojson_validate_geometry` détermine si le format applique les règles de structure géométrique de la [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1), dans les deux sens. Il est activé par défaut.

Lorsqu’il est activé, toute géométrie qui ne respecte pas les règles de structure GeoJSON est rejetée : un `LineString` (ou une ligne d’un `MultiLineString`) avec moins de deux points ; un anneau de `Polygon` ou de `MultiPolygon` avec moins de quatre points, ou dont le premier et le dernier point diffèrent (anneau non fermé) ; ou un `MultiLineString`, `Polygon` ou `MultiPolygon` vide. Les mêmes règles s’appliquent aussi bien à la lecture d’un tel document qu’à l’écriture d’une telle valeur ClickHouse ; ainsi, un document écrit pourra toujours être relu.

Lorsqu’il est désactivé, ces règles de structure ne sont appliquées dans aucun des deux sens : les géométries dégénérées sont lues telles quelles et écrites telles quelles. Cela permet à des valeurs géométriques ClickHouse qui ne sont pas des géométries GeoJSON valides d’effectuer un aller-retour via ce format, au prix de produire des documents GeoJSON non valides.

La validation est uniquement structurelle : elle vérifie le nombre de points et la fermeture des anneaux. Elle ne contrôle pas la validité géométrique d’une forme ; une géométrie structurellement valide mais géométriquement dégénérée est donc acceptée dans les deux sens — par exemple, un polygone d’aire nulle, un anneau auto-intersectant ou un polygone dont les trous (anneaux intérieurs) se trouvent en dehors de l’anneau extérieur. De même, l’orientation des anneaux de polygone selon la règle de la main droite (`winding`) n’est jamais imposée.

Une vérification est indépendante du paramètre : les coordonnées non finies (`NaN`, `Inf`) sont toujours rejetées, car elles ne peuvent pas être représentées sous forme de nombres JSON.