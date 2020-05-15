---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 59
toc_title: "Yandex\u306E\u3067\u306E\u4F5C\u696D\u3002\u30E1\u30C8\u30EA\u30AB\u8F9E\
  \u66F8"
---

# Yandexで作業するための機能。メトリカ辞書 {#functions-for-working-with-yandex-metrica-dictionaries}

以下の機能が機能するためには、サーバー設定はすべてのyandexを取得するためのパスとアドレスを指定する必要があります。メトリカ辞書。 辞書は、これらの関数の最初の呼び出し時にロードされます。 参照リストをロードできない場合は、例外がスローされます。

のための情報を参照リストの項をご参照ください “Dictionaries”.

## 複数のジオベース {#multiple-geobases}

ClickHouseは、複数の代替ジオベース（地域階層）を同時に使用して、特定の地域が属する国のさまざまな視点をサポートします。

その ‘clickhouse-server’ configは、地域階層を持つファイルを指定します::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

このファイルのほかに、それはまた、（ファイル拡張子の前に）\_シンボルと名前に追加任意の接尾辞を持っている近くのファイルを検索します。
たとえば、ファイルも検索します `/opt/geo/regions_hierarchy_ua.txt`、もしあれば。

`ua` 辞書キーと呼ばれます。 接尾辞のない辞書の場合、キーは空の文字列です。

すべての辞書は実行時に再ロードされます（builtin\_dictionaries\_reload\_interval設定パラメータで定義されているすべての秒数、またはデフォルトで時間が一度）。 ただし、使用可能な辞書のリストは、サーバーの起動時に一度だけ定義されます。

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
例えば:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-faq\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### regionToArea(id\[,geobase\]) {#regiontoareaid-geobase}

領域を領域に変換します(ジオベースのタイプ5)。 他のすべての方法では、この関数は次のようになります ‘regionToCity’.

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

### ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-ﾂ篠堕猟ｿﾂ青ｿﾂ仰\]) {#regiontodistrictid-geobase}

地域を連邦区(ジオベースのタイプ4)に変換します。 他のすべての方法では、この関数は次のようになります ‘regionToCity’.

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

### ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-ﾂつｲﾂ堕環談\]) {#regiontocountryid-geobase}

地域を国に変換します。 他のすべての方法では、この関数は次のようになります ‘regionToCity’.
例えば: `regionToCountry(toUInt32(213)) = 225` モスクワ（213）をロシア（225）に変換する。

### ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-ﾂつｲﾂ堕環談\]) {#regiontocontinentid-geobase}

地域を大陸に変換します。 他のすべての方法では、この関数は次のようになります ‘regionToCity’.
例えば: `regionToContinent(toUInt32(213)) = 10001` モスクワ（213）をユーラシア（10001）に変換する。

### regionToTopContinent(\#regiontotopcontinent) {#regiontotopcontinent-regiontotopcontinent}

リージョンの階層で最上位の大陸を検索します。

**構文**

``` sql
regionToTopContinent(id[, geobase]);
```

**パラメータ**

-   `id` — Region ID from the Yandex geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [複数のジオベース](#multiple-geobases). [文字列](../../sql-reference/data-types/string.md). 任意です。

**戻り値**

-   トップレベルの大陸の識別子（後者は地域の階層を登るとき）。
-   0、何もない場合。

タイプ: `UInt32`.

### ﾂ環板篠ｮﾂ嘉ｯﾂ偲青エﾂδﾂ-ﾂエﾂスﾂ-ﾂシﾂ\]) {#regiontopopulationid-geobase}

地域の人口を取得します。
人口はgeobaseのファイルに記録することができます。 セクションを見る “External dictionaries”.
リージョンに対して母集団が記録されていない場合は、0を返します。
Yandex geobaseでは、母集団は子地域に対して記録されますが、親地域に対しては記録されません。

### ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹﾂ-ﾂ篠堕猟ｿﾂ青ｿﾂ仰\]) {#regioninlhs-rhs-geobase}

をチェックする。 ‘lhs’ リージョンは ‘rhs’ 地域。 UInt8が属している場合は1、属していない場合は0を返します。
The relationship is reflexive – any region also belongs to itself.

### ﾂ環板篠ｮﾂ嘉ｯﾂ偲青エﾂδﾂ-ﾂエﾂスﾂ-ﾂシﾂ\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
例えば: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### リージョン名(id\[,lang\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn’t exist, an empty string is returned.

`ua` と `uk` もうクです。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
