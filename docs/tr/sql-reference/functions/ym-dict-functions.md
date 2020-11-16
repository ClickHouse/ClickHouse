---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "Yandex ile \xE7al\u0131\u015Fmak.Metrica S\xF6zl\xFCkleri"
---

# Yandex ile çalışmak için fonksiyonlar.Metrica Sözlükleri {#functions-for-working-with-yandex-metrica-dictionaries}

Aşağıdaki işlevlerin çalışması için, sunucu yapılandırmasının tüm Yandex'i almak için yolları ve adresleri belirtmesi gerekir.Metrica sözlükler. Sözlükler, bu işlevlerden herhangi birinin ilk çağrısında yüklenir. Başvuru listeleri yüklenemiyorsa, bir özel durum atılır.

Başvuru listeleri oluşturma hakkında daha fazla bilgi için bölüme bakın “Dictionaries”.

## Çoklu Geobazlar {#multiple-geobases}

ClickHouse, belirli bölgelerin hangi ülkelere ait olduğu konusunda çeşitli perspektifleri desteklemek için aynı anda birden fazla alternatif jeobaz (bölgesel hiyerarşiler) ile çalışmayı destekler.

Bu ‘clickhouse-server’ config, dosyayı bölgesel hiyerarşi ile belirtir::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Bu dosyanın yanı sıra, yakındaki _ sembolüne ve isme eklenen herhangi bir sonek (dosya uzantısından önce) olan dosyaları da arar.
Örneğin, dosyayı da bulacaktır `/opt/geo/regions_hierarchy_ua.txt` varsa.

`ua` sözlük anahtarı denir. Soneksiz bir sözlük için anahtar boş bir dizedir.

Tüm sözlükler çalışma zamanında yeniden yüklenir (buıltın_dıctıonarıes_reload_ınterval yapılandırma parametresinde tanımlandığı gibi belirli sayıda saniyede bir kez veya varsayılan olarak saatte bir kez). Ancak, sunucu başladığında kullanılabilir sözlüklerin listesi bir kez tanımlanır.

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
Örnek:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToCity (id \[, geobase\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### regionToArea (id \[, geobase\]) {#regiontoareaid-geobase}

Bir bölgeyi bir alana dönüştürür (geobase içinde 5 yazın). Diğer her şekilde, bu işlev aynıdır ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
```

### regionToDistrict (id \[, geobase\]) {#regiontodistrictid-geobase}

Bir bölgeyi federal bir bölgeye dönüştürür (geobase içinde tip 4). Diğer her şekilde, bu işlev aynıdır ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
┌─regionToName(regionToDistrict(toUInt32(number), \'ua\'))─┐
│                                                          │
│ Central federal district                                 │
│ Northwest federal district                               │
│ South federal district                                   │
│ North Caucases federal district                          │
│ Privolga federal district                                │
│ Ural federal district                                    │
│ Siberian federal district                                │
│ Far East federal district                                │
│ Scotland                                                 │
│ Faroe Islands                                            │
│ Flemish region                                           │
│ Brussels capital region                                  │
│ Wallonia                                                 │
│ Federation of Bosnia and Herzegovina                     │
└──────────────────────────────────────────────────────────┘
```

### regionToCountry (ıd \[, geobase\]) {#regiontocountryid-geobase}

Bir bölgeyi bir ülkeye dönüştürür. Diğer her şekilde, bu işlev aynıdır ‘regionToCity’.
Örnek: `regionToCountry(toUInt32(213)) = 225` Moskova'yı (213) Rusya'ya (225) dönüştürür.

### regionToContinent (id \[, geobase\]) {#regiontocontinentid-geobase}

Bir bölgeyi bir kıtaya dönüştürür. Diğer her şekilde, bu işlev aynıdır ‘regionToCity’.
Örnek: `regionToContinent(toUInt32(213)) = 10001` Moskova'yı (213) Avrasya'ya (10001) dönüştürür.

### regionToTopContinent (#regiontotopcontinent) {#regiontotopcontinent-regiontotopcontinent}

Bölgenin hiyerarşisinde en yüksek kıtayı bulur.

**Sözdizimi**

``` sql
regionToTopContinent(id[, geobase]);
```

**Parametre**

-   `id` — Region ID from the Yandex geobase. [Uİnt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [Çoklu Geobazlar](#multiple-geobases). [Dize](../../sql-reference/data-types/string.md). İsteğe bağlı.

**Döndürülen değer**

-   Üst düzey kıtanın tanımlayıcısı (bölgeler hiyerarşisine tırmandığınızda ikincisi).
-   0, yoksa.

Tür: `UInt32`.

### regionToPopulation (id \[, geobase\]) {#regiontopopulationid-geobase}

Bir bölge için nüfusu alır.
Nüfus geobase ile dosyalarda kaydedilebilir. Bölümüne bakınız “External dictionaries”.
Bölge için nüfus kaydedilmezse, 0 döndürür.
Yandex geobase'de, nüfus alt bölgeler için kaydedilebilir, ancak üst bölgeler için kaydedilemez.

### regionİn (lhs, rhs \[, geobase\]) {#regioninlhs-rhs-geobase}

Olup olmadığını denetler bir ‘lhs’ bölge bir ‘rhs’ bölge. Aitse 1'e eşit bir Uİnt8 numarası veya ait değilse 0 döndürür.
The relationship is reflexive – any region also belongs to itself.

### regionHierarchy (id \[, geobase\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
Örnek: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### regionToName (id \[, lang\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn't exist, an empty string is returned.

`ua` ve `uk` hem Ukrayna demek.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
