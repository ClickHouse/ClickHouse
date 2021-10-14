---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "Travailler avec des coordonn\xE9es g\xE9ographiques"
---

# Fonctions pour travailler avec des coordonnées géographiques {#functions-for-working-with-geographical-coordinates}

## greatCircleDistance {#greatcircledistance}

Calculer la distance entre deux points sur la surface de la Terre en utilisant [la formule du grand cercle](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Les paramètres d'entrée**

-   `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
-   `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
-   `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
-   `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

Les valeurs positives correspondent à la latitude nord et à la longitude Est, et les valeurs négatives à la latitude Sud et à la longitude ouest.

**Valeur renvoyée**

La distance entre deux points sur la surface de la Terre, en mètres.

Génère une exception lorsque les valeurs des paramètres d'entrée se situent en dehors de la plage.

**Exemple**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

``` text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## pointInEllipses {#pointinellipses}

Vérifie si le point appartient à au moins une des ellipses.
Coordonnées géométriques sont dans le système de coordonnées Cartésiennes.

``` sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Les paramètres d'entrée**

-   `x, y` — Coordinates of a point on the plane.
-   `xᵢ, yᵢ` — Coordinates of the center of the `i`-ème points de suspension.
-   `aᵢ, bᵢ` — Axes of the `i`- e ellipse en unités de coordonnées x, Y.

Les paramètres d'entrée doivent être `2+4⋅n`, où `n` est le nombre de points de suspension.

**Valeurs renvoyées**

`1` si le point est à l'intérieur d'au moins l'un des ellipses; `0`si elle ne l'est pas.

**Exemple**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

``` text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## pointtinpolygon {#pointinpolygon}

Vérifie si le point appartient au polygone sur l'avion.

``` sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Les valeurs d'entrée**

-   `(x, y)` — Coordinates of a point on the plane. Data type — [Tuple](../../sql-reference/data-types/tuple.md) — A tuple of two numbers.
-   `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [Tableau](../../sql-reference/data-types/array.md). Chaque sommet est représenté par une paire de coordonnées `(a, b)`. Les sommets doivent être spécifiés dans le sens horaire ou antihoraire. Le nombre minimum de sommets est 3. Le polygone doit être constante.
-   La fonction prend également en charge les polygones avec des trous (découper des sections). Dans ce cas, ajoutez des polygones qui définissent les sections découpées en utilisant des arguments supplémentaires de la fonction. La fonction ne prend pas en charge les polygones non simplement connectés.

**Valeurs renvoyées**

`1` si le point est à l'intérieur du polygone, `0` si elle ne l'est pas.
Si le point est sur la limite du polygone, la fonction peut renvoyer 0 ou 1.

**Exemple**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode {#geohashencode}

Encode la latitude et la longitude en tant que chaîne geohash, voir (http://geohash.org/, https://en.wikipedia.org/wiki/Geohash).

``` sql
geohashEncode(longitude, latitude, [precision])
```

**Les valeurs d'entrée**

-   longitude longitude partie de la coordonnée que vous souhaitez encoder. Flottant dans la gamme`[-180°, 180°]`
-   latitude latitude partie de la coordonnée que vous souhaitez encoder. Flottant dans la gamme `[-90°, 90°]`
-   precision-facultatif, longueur de la chaîne codée résultante, par défaut `12`. Entier dans la gamme `[1, 12]`. Toute valeur inférieure à `1` ou supérieure à `12` silencieusement converti à `12`.

**Valeurs renvoyées**

-   alphanumérique `String` de coordonnées codées (la version modifiée de l'alphabet de codage base32 est utilisée).

**Exemple**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode {#geohashdecode}

Décode toute chaîne codée geohash en longitude et latitude.

**Les valeurs d'entrée**

-   chaîne codée-chaîne codée geohash.

**Valeurs renvoyées**

-   (longitude, latitude) - 2-n-uplet de `Float64` les valeurs de longitude et de latitude.

**Exemple**

``` sql
SELECT geohashDecode('ezs42') AS res
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geoToH3 {#geotoh3}

Retourner [H3](https://uber.github.io/h3/#/documentation/overview/introduction) point d'indice `(lon, lat)` avec une résolution spécifiée.

[H3](https://uber.github.io/h3/#/documentation/overview/introduction) est un système d'indexation géographique où la surface de la Terre divisée en carreaux hexagonaux même. Ce système est hiérarchique, c'est-à-dire que chaque hexagone au niveau supérieur peut être divisé en sept, même mais plus petits, etc.

Cet indice est principalement utilisé pour les emplacements de bucketing et d'autres manipulations géospatiales.

**Syntaxe**

``` sql
geoToH3(lon, lat, resolution)
```

**Paramètre**

-   `lon` — Longitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `lat` — Latitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Numéro d'indice hexagonal.
-   0 en cas d'erreur.

Type: `UInt64`.

**Exemple**

Requête:

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```

Résultat:

``` text
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## geohashesInBox {#geohashesinbox}

Renvoie un tableau de chaînes codées geohash de précision donnée qui tombent à l'intérieur et croisent les limites d'une boîte donnée, essentiellement une grille 2D aplatie en tableau.

**Les valeurs d'entrée**

-   longitude_min-longitude min, valeur flottante dans la plage `[-180°, 180°]`
-   latitude_min-latitude min, valeur flottante dans la plage `[-90°, 90°]`
-   longitude_max-longitude maximale, valeur flottante dans la plage `[-180°, 180°]`
-   latitude_max-latitude maximale, valeur flottante dans la plage `[-90°, 90°]`
-   précision - geohash précision, `UInt8` dans la gamme `[1, 12]`

Veuillez noter que tous les paramètres de coordonnées doit être du même type: soit `Float32` ou `Float64`.

**Valeurs renvoyées**

-   gamme de précision de longues chaînes de geohash-boîtes couvrant la zone, vous ne devriez pas compter sur l'ordre des éléments.
-   \[\] - tableau vide si *min* les valeurs de *latitude* et *longitude* ne sont pas moins de correspondant *Max* valeur.

Veuillez noter que la fonction lancera une exception si le tableau résultant a plus de 10'000'000 éléments.

**Exemple**

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

## h3GetBaseCell {#h3getbasecell}

Renvoie le numéro de cellule de base de l'index.

**Syntaxe**

``` sql
h3GetBaseCell(index)
```

**Paramètre**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Numéro de cellule de base hexagonale. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT h3GetBaseCell(612916788725809151) as basecell
```

Résultat:

``` text
┌─basecell─┐
│       12 │
└──────────┘
```

## h3HexAreaM2 {#h3hexaream2}

Surface hexagonale Moyenne en mètres carrés à la résolution donnée.

**Syntaxe**

``` sql
h3HexAreaM2(resolution)
```

**Paramètre**

-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Area in m². Type: [Float64](../../sql-reference/data-types/float.md).

**Exemple**

Requête:

``` sql
SELECT h3HexAreaM2(13) as area
```

Résultat:

``` text
┌─area─┐
│ 43.9 │
└──────┘
```

## h3IndexesAreNeighbors {#h3indexesareneighbors}

Renvoie si les H3Indexes fournis sont voisins ou non.

**Syntaxe**

``` sql
h3IndexesAreNeighbors(index1, index2)
```

**Paramètre**

-   `index1` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `index2` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Retourner `1` si les index sont voisins, `0` autrement. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359) AS n
```

Résultat:

``` text
┌─n─┐
│ 1 │
└───┘
```

## h3enfants {#h3tochildren}

Retourne un tableau avec les index enfants de l'index donné.

**Syntaxe**

``` sql
h3ToChildren(index, resolution)
```

**Paramètre**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Tableau avec les index H3 enfants. Tableau de type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT h3ToChildren(599405990164561919, 6) AS children
```

Résultat:

``` text
┌─children───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## h3ToParent {#h3toparent}

Renvoie l'index parent (plus grossier) contenant l'index donné.

**Syntaxe**

``` sql
h3ToParent(index, resolution)
```

**Paramètre**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Parent H3 index. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT h3ToParent(599405990164561919, 3) as parent
```

Résultat:

``` text
┌─────────────parent─┐
│ 590398848891879423 │
└────────────────────┘
```

## h3ToString {#h3tostring}

Convertit la représentation H3Index de l'index en représentation de chaîne.

``` sql
h3ToString(index)
```

**Paramètre**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   Représentation en chaîne de l'index H3. Type: [Chaîne](../../sql-reference/data-types/string.md).

**Exemple**

Requête:

``` sql
SELECT h3ToString(617420388352917503) as h3_string
```

Résultat:

``` text
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
```

## stringToH3 {#stringtoh3}

Convertit la représentation de chaîne en représentation H3Index (UInt64).

``` sql
stringToH3(index_str)
```

**Paramètre**

-   `index_str` — String representation of the H3 index. Type: [Chaîne](../../sql-reference/data-types/string.md).

**Valeurs renvoyées**

-   Numéro d'indice hexagonal. Renvoie 0 en cas d'erreur. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT stringToH3('89184926cc3ffff') as index
```

Résultat:

``` text
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
```

## h3grésolution {#h3getresolution}

Retourne la résolution de l'index.

**Syntaxe**

``` sql
h3GetResolution(index)
```

**Paramètre**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Valeurs renvoyées**

-   L'indice de la résolution. Gamme: `[0, 15]`. Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT h3GetResolution(617420388352917503) as res
```

Résultat:

``` text
┌─res─┐
│   9 │
└─────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/geo/) <!--hide-->
