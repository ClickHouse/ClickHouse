---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "Co\u011Frafi koordinatlar ile \xE7al\u0131\u015Fma"
---

# Coğrafi Koordinatlarla çalışmak için fonksiyonlar {#functions-for-working-with-geographical-coordinates}

## greatCircleDistance {#greatcircledistance}

Dünya yüzeyindeki iki nokta arasındaki mesafeyi kullanarak hesaplayın [büyük daire formülü](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**Giriş parametreleri**

-   `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
-   `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
-   `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
-   `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

Pozitif değerler Kuzey enlemi ve Doğu boylamına karşılık gelir ve negatif değerler Güney enlemi ve Batı boylamına karşılık gelir.

**Döndürülen değer**

Dünya yüzeyindeki iki nokta arasındaki mesafe, metre cinsinden.

Girdi parametre değerleri aralığın dışına düştüğünde bir özel durum oluşturur.

**Örnek**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

``` text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## pointİnEllipses {#pointinellipses}

Noktanın elipslerden en az birine ait olup olmadığını kontrol eder.
Koordinatlar kartezyen koordinat sisteminde geometriktir.

``` sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**Giriş parametreleri**

-   `x, y` — Coordinates of a point on the plane.
-   `xᵢ, yᵢ` — Coordinates of the center of the `i`-inci üç nokta.
-   `aᵢ, bᵢ` — Axes of the `i`- x, y koordinatları birimlerinde üç nokta.

Giriş parametreleri olmalıdır `2+4⋅n`, nere `n` elips sayısıdır.

**Döndürülen değerler**

`1` nokta elipslerden en az birinin içindeyse; `0`hayır değil.

**Örnek**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

``` text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## pointİnPolygon {#pointinpolygon}

Noktanın düzlemdeki poligona ait olup olmadığını kontrol eder.

``` sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**Giriş değerleri**

-   `(x, y)` — Coordinates of a point on the plane. Data type — [Demet](../../sql-reference/data-types/tuple.md) — A tuple of two numbers.
-   `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [Dizi](../../sql-reference/data-types/array.md). Her köşe bir çift koordinat ile temsil edilir `(a, b)`. Köşeler saat yönünde veya saat yönünün tersine sırayla belirtilmelidir. Minimum köşe sayısı 3'tür. Çokgen sabit olmalıdır.
-   Fonksiyon ayrıca delikli çokgenleri de destekler (bölümleri keser). Bu durumda, işlevin ek argümanlarını kullanarak kesilen bölümleri tanımlayan çokgenler ekleyin. İşlev, basit olmayan bağlı çokgenleri desteklemez.

**Döndürülen değerler**

`1` nokta çokgenin içinde ise, `0` hayır değil.
Nokta çokgen sınırında ise, işlev 0 veya 1 döndürebilir.

**Örnek**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## geohashEncode {#geohashencode}

Enlem ve boylamı bir geohash-string olarak kodlar, Lütfen bakınız (http://geohash.org/, https://en.wikipedia.org/wiki/Geohash).

``` sql
geohashEncode(longitude, latitude, [precision])
```

**Giriş değerleri**

-   boylam-kodlamak istediğiniz koordinatın boylam kısmı. Aralık floatingta yüz floatingen`[-180°, 180°]`
-   latitude-kodlamak istediğiniz koordinatın enlem kısmı. Aralık floatingta yüz floatingen `[-90°, 90°]`
-   hassas-isteğe bağlı, elde edilen kodlanmış dizenin uzunluğu, varsayılan olarak `12`. Aralıktaki tamsayı `[1, 12]`. Herhangi bir değer daha az `1` veya daha büyük `12` sessizce dönüştürülür `12`.

**Döndürülen değerler**

-   alfanümerik `String` kodlanmış koordinat (base32-kodlama alfabesinin değiştirilmiş versiyonu kullanılır).

**Örnek**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## geohashDecode {#geohashdecode}

Herhangi bir geohash kodlu dizeyi boylam ve enlem olarak çözer.

**Giriş değerleri**

-   kodlanmış dize-geohash kodlanmış dize.

**Döndürülen değerler**

-   (boylam, enlem) - 2-tuple `Float64` boylam ve enlem değerleri.

**Örnek**

``` sql
SELECT geohashDecode('ezs42') AS res
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## geoToH3 {#geotoh3}

Dönüşler [H3](https://uber.github.io/h3/#/documentation/overview/introduction) nokta Endeksi `(lon, lat)` belirtilen çözünürlük ile.

[H3](https://uber.github.io/h3/#/documentation/overview/introduction) Dünya yüzeyinin altıgen fayanslara bile bölündüğü coğrafi bir indeksleme sistemidir. Bu sistem hiyerarşiktir, örn. üst seviyedeki her altıgen yedi hatta daha küçük olanlara bölünebilir.

Bu endeks öncelikle kovalama yerleri ve diğer coğrafi manipülasyonlar için kullanılır.

**Sözdizimi**

``` sql
geoToH3(lon, lat, resolution)
```

**Parametre**

-   `lon` — Longitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `lat` — Latitude. Type: [Float64](../../sql-reference/data-types/float.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Altıgen dizin numarası.
-   Hata durumunda 0.

Tür: `UInt64`.

**Örnek**

Sorgu:

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```

Sonuç:

``` text
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## geohashesİnBox {#geohashesinbox}

Verilen kutunun içine giren ve verilen kutunun sınırlarını kesişen, temel olarak diziye düzleştirilmiş bir 2D ızgarası olan bir dizi geohash kodlu dizeler dizisi döndürür.

**Giriş değerleri**

-   longitude\_min - min boylam, aralıkta kayan değer `[-180°, 180°]`
-   latitude\_min - min enlem, aralıkta kayan değer `[-90°, 90°]`
-   longitude\_max-maksimum boylam, aralıkta kayan değer `[-180°, 180°]`
-   latitude\_max-maksimum enlem, aralıkta kayan değer `[-90°, 90°]`
-   hassas-geohash hassas, `UInt8` Aralık inta `[1, 12]`

Lütfen tüm koordinat parametrelerinin aynı tipte olması gerektiğini unutmayın: `Float32` veya `Float64`.

**Döndürülen değerler**

-   verilen alanı kapsayan geohash kutularının hassas uzun dizeleri dizisi, öğelerin sırasına güvenmemelisiniz.
-   \[\]- eğer boş dizi *dakika* değerleri *enlem* ve *Boylam* karşılık gelenden daha az değil *maksimum* değerler.

Ortaya çıkan dizi 10'000' 000 ürün uzunluğundaysa, işlevin bir istisna atacağını lütfen unutmayın.

**Örnek**

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

## h3GetBaseCell {#h3getbasecell}

Dizin temel hücre numarasını döndürür.

**Sözdizimi**

``` sql
h3GetBaseCell(index)
```

**Parametre**

-   `index` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Altıgen baz hücre numarası. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT h3GetBaseCell(612916788725809151) as basecell
```

Sonuç:

``` text
┌─basecell─┐
│       12 │
└──────────┘
```

## h3HexAreaM2 {#h3hexaream2}

Verilen çözünürlükte metrekare ortalama altıgen alan.

**Sözdizimi**

``` sql
h3HexAreaM2(resolution)
```

**Parametre**

-   `resolution` — Index resolution. Range: `[0, 15]`. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Area in m². Type: [Float64](../../sql-reference/data-types/float.md).

**Örnek**

Sorgu:

``` sql
SELECT h3HexAreaM2(13) as area
```

Sonuç:

``` text
┌─area─┐
│ 43.9 │
└──────┘
```

## h3İndexesAreNeighbors {#h3indexesareneighbors}

Sağlanan H3indexlerin komşu olup olmadığını döndürür.

**Sözdizimi**

``` sql
h3IndexesAreNeighbors(index1, index2)
```

**Parametre**

-   `index1` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).
-   `index2` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Dönüşler `1` dizinler komşu ise, `0` başka. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359) AS n
```

Sonuç:

``` text
┌─n─┐
│ 1 │
└───┘
```

## h3ToChildren {#h3tochildren}

Verilen dizinin alt dizinlerini içeren bir dizi döndürür.

**Sözdizimi**

``` sql
h3ToChildren(index, resolution)
```

**Parametre**

-   `index` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Alt H3 dizinleri ile dizi. Dizi türü: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT h3ToChildren(599405990164561919, 6) AS children
```

Sonuç:

``` text
┌─children───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## h3ToParent {#h3toparent}

Verilen dizini içeren üst (kaba) dizini döndürür.

**Sözdizimi**

``` sql
h3ToParent(index, resolution)
```

**Parametre**

-   `index` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Ana H3 Endeksi. Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT h3ToParent(599405990164561919, 3) as parent
```

Sonuç:

``` text
┌─────────────parent─┐
│ 590398848891879423 │
└────────────────────┘
```

## h3ToString {#h3tostring}

Dizinin H3ındex gösterimini dize gösterimine dönüştürür.

``` sql
h3ToString(index)
```

**Parametre**

-   `index` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   H3 dizininin dize gösterimi. Tür: [Dize](../../sql-reference/data-types/string.md).

**Örnek**

Sorgu:

``` sql
SELECT h3ToString(617420388352917503) as h3_string
```

Sonuç:

``` text
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
```

## stringToH3 {#stringtoh3}

Dize gösterimini H3ındex (Uİnt64) gösterimine dönüştürür.

``` sql
stringToH3(index_str)
```

**Parametre**

-   `index_str` — String representation of the H3 index. Type: [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değerler**

-   Altıgen dizin numarası. Hata 0 döndürür. Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT stringToH3('89184926cc3ffff') as index
```

Sonuç:

``` text
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
```

## h3GetResolution {#h3getresolution}

Dizin çözünürlüğünü döndürür.

**Sözdizimi**

``` sql
h3GetResolution(index)
```

**Parametre**

-   `index` — Hexagon index number. Type: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Döndürülen değerler**

-   Dizin çözünürlüğü. Aralık: `[0, 15]`. Tür: [Uİnt8](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT h3GetResolution(617420388352917503) as res
```

Sonuç:

``` text
┌─res─┐
│   9 │
└─────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/geo/) <!--hide-->
