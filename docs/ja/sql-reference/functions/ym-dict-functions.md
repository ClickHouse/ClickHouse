---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "Yandex\u3067\u306E\u4F5C\u696D\u3002\u30E1\u30C8\u30EA\u30AB\u8F9E\u66F8"
---

# Yandexで作業するための機能。メトリカ辞書 {#functions-for-working-with-yandex-metrica-dictionaries}

以下の機能を機能させるには、サーバー設定ですべてのYandexを取得するためのパスとアドレスを指定する必要があります。メトリカ辞書。 辞書は、これらの関数の最初の呼び出し時に読み込まれます。 参照リストをロードできない場合は、例外がスローされます。

のための情報を参照リストの項をご参照ください “Dictionaries”.

## 複数のジオベース {#multiple-geobases}

ClickHouseは、特定の地域が属する国のさまざまな視点をサポートするために、複数の代替ジオベース（地域階層）を同時に使用することをサポートしています。

その ‘clickhouse-server’ configは、地域階層を持つファイルを指定します::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

このファイル以外にも、名前に_記号と接尾辞が付加された近くのファイルも検索します（ファイル拡張子の前に）。
たとえば、次のファイルも検索します `/opt/geo/regions_hierarchy_ua.txt`、存在する場合。

`ua` 辞書キーと呼ばれます。 接尾辞のない辞書の場合、キーは空の文字列です。

すべての辞書は実行時に再ロードされます(builtin_dictionaries_reload_interval設定パラメータで定義されているように、一定の秒数ごとに、またはデフォルトでは時間に一度)。 ただし、使用可能な辞書のリストは、サーバーの起動時に一度に定義されます。

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
例:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### リージョントシティ(id\[,geobase\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### レギオントアレア(id\[,geobase\]) {#regiontoareaid-geobase}

領域を領域に変換します(ジオベースのタイプ5)。 他のすべての方法では、この関数は次のように同じです ‘regionToCity’.

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

### レジオントディストリクト(id\[,geobase\]) {#regiontodistrictid-geobase}

地域を連邦区（ジオベースのタイプ4）に変換します。 他のすべての方法では、この関数は次のように同じです ‘regionToCity’.

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

### regionToCountry(id\[,geobase\]) {#regiontocountryid-geobase}

地域を国に変換します。 他のすべての方法では、この関数は次のように同じです ‘regionToCity’.
例: `regionToCountry(toUInt32(213)) = 225` モスクワ（213）をロシア（225）に変換します。

### レジオントコンテンツ(id\[,geobase\]) {#regiontocontinentid-geobase}

地域を大陸に変換します。 他のすべての方法では、この関数は次のように同じです ‘regionToCity’.
例: `regionToContinent(toUInt32(213)) = 10001` モスクワ(213)をユーラシア(10001)に変換します。

### regionToTopContinent(#regiontotopcontinent) {#regiontotopcontinent-regiontotopcontinent}

リージョンの階層内で最も高い大陸を検索します。

**構文**

``` sql
regionToTopContinent(id[, geobase]);
```

**パラメータ**

-   `id` — Region ID from the Yandex geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [複数のジオベース](#multiple-geobases). [文字列](../../sql-reference/data-types/string.md). 任意。

**戻り値**

-   トップレベルの大陸の識別子（後者は地域の階層を登るとき）。
-   ない場合は0。

タイプ: `UInt32`.

### リージョントポピュレーション(id\[,geobase\]) {#regiontopopulationid-geobase}

地域の人口を取得します。
母集団は、ジオベースを持つファイルに記録することができます。 セクションを参照 “External dictionaries”.
人口が地域に記録されていない場合は、0を返します。
Yandexジオベースでは、子リージョンに対して母集団が記録されますが、親リージョンに対しては記録されません。

### レギオニン(lhs,rhs\[,geobase\]) {#regioninlhs-rhs-geobase}

Aかどうかチェック ‘lhs’ 地域はaに属します ‘rhs’ 地域。 UInt8が属している場合は1、属していない場合は0を返します。
The relationship is reflexive – any region also belongs to itself.

### リージョンハイアーキテクチャ(id\[,geobase\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
例: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### レジオントナム(id\[,lang\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn't exist, an empty string is returned.

`ua` と `uk` もうクです。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
