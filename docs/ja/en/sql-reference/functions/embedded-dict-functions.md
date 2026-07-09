---
description: '埋め込み Dictionary を操作する関数に関するドキュメント'
sidebar_label: '埋め込み Dictionary'
slug: /sql-reference/functions/ym-dict-functions
title: '埋め込み Dictionary を操作する関数'
doc_type: 'reference'
---

:::note
以下の関数を動作させるには、すべての埋め込み Dictionary を取得するためのパスとアドレスをサーバーの config で指定する必要があります。これらの Dictionary は、いずれかの関数が最初に呼び出されたときに読み込まれます。参照リストを読み込めない場合は、例外がスローされます。

そのため、このセクションの例は、事前に設定しない限り、デフォルトでは [ClickHouse Fiddle](https://fiddle.clickhouse.com/) と quick release および production のデプロイメントで例外をスローします。
:::

参照リストの作成については、[&quot;Dictionaries&quot;](../statements/create/dictionary/embedded) セクションを参照してください。

<div id="multiple-geobases">
  ## 複数のジオベース
</div>

ClickHouse は、特定の地域がどの国に属するかについてのさまざまな見方に対応するため、複数の代替 geobase (地域階層) を同時に扱うことをサポートしています。

`clickhouse-server` の設定では、地域階層のファイルを指定します。

`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

このファイルに加えて、同じ場所にあり、ファイル名に `_` 記号と任意の接尾辞が付いたファイル (拡張子の前) も検索されます。
たとえば、`/opt/geo/regions_hierarchy_ua.txt` というファイルが存在すれば、それも検出されます。ここで `ua` は辞書キーと呼ばれます。接尾辞のない辞書のキーは空文字列です。

すべての辞書は実行時に再読み込みされます ([`builtin_dictionaries_reload_interval`](/ja/operations/server-configuration-parameters/settings#builtin_dictionaries_reload_interval) 設定パラメータで定義された一定秒数ごと、またはデフォルトで 1 時間ごと) 。ただし、利用可能な辞書の一覧は、サーバーの起動時に一度だけ定義されます。

地域を扱うすべての関数では、末尾に省略可能な引数として辞書キーを指定できます。これは geobase と呼ばれます。

例:

```sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

リージョン ID と geobase を受け取り、対応する言語でそのリージョン名を表す文字列を返します。指定した ID のリージョンが存在しない場合は、空文字列が返されます。

**構文**

```sql
regionToName(id\[, lang\])
```

**パラメータ**

* `id` — geobase の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能。

**戻り値**

* `geobase` で指定された対応する言語でのリージョン名。[String](../data-types/string)。
* それ以外の場合は空文字列。

**例**

```sql title="Query"
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```

### regionToCity

geobase の region ID を受け取ります。この region が都市または都市の一部である場合、対応する都市の region ID を返します。そうでない場合は 0 を返します。

**構文**

```sql
regionToCity(id [, geobase])
```

**パラメータ**

* `id` — geobase の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 該当する都市が存在する場合は、その Region ID。[UInt32](../data-types/int-uint)。
* 存在しない場合は 0。

**例**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```response title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCity(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                          │
│ World                                      │  0 │                                                          │
│ USA                                        │  0 │                                                          │
│ Colorado                                   │  0 │                                                          │
│ Boulder County                             │  0 │                                                          │
│ Boulder                                    │  5 │ Boulder                                                  │
│ China                                      │  0 │                                                          │
│ Sichuan                                    │  0 │                                                          │
│ Chengdu                                    │  8 │ Chengdu                                                  │
│ America                                    │  0 │                                                          │
│ North America                              │  0 │                                                          │
│ Eurasia                                    │  0 │                                                          │
│ Asia                                       │  0 │                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────┘
```

### regionToArea

region を area (geobase の type 5) に変換します。その他の点については、この関数は [&#39;regionToCity&#39;](#regiontocity) と同じです。

**構文**

```sql
regionToArea(id [, geobase])
```

**パラメーター**

* `id` — geobase の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 対応する地域が存在する場合は、その Region ID。[UInt32](../data-types/int-uint)。
* 存在しない場合は 0。

**例**

```sql title="Query"
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
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

### regionToDistrict

region を連邦管区 (geobase の type 4) に変換します。その他の点では、この関数は &#39;regionToCity&#39; と同じです。

**構文**

```sql
regionToDistrict(id [, geobase])
```

**パラメータ**

* `id` — geobase の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 対応する都市が存在する場合は、その Region ID。[UInt32](../data-types/int-uint)。
* 存在しない場合は 0。

**例**

```sql title="Query"
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
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

### regionToCountry

region を国 (geobase の type 3) に変換します。それ以外は、この関数は &#39;regionToCity&#39; と同じです。

**構文**

```sql
regionToCountry(id [, geobase])
```

**パラメータ**

* `id` — geobase の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 存在する場合は、対応する国の Region ID。[UInt32](../data-types/int-uint)。
* 存在しない場合は 0。

**例**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCountry(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                             │
│ World                                      │  0 │                                                             │
│ USA                                        │  2 │ USA                                                         │
│ Colorado                                   │  2 │ USA                                                         │
│ Boulder County                             │  2 │ USA                                                         │
│ Boulder                                    │  2 │ USA                                                         │
│ China                                      │  6 │ China                                                       │
│ Sichuan                                    │  6 │ China                                                       │
│ Chengdu                                    │  6 │ China                                                       │
│ America                                    │  0 │                                                             │
│ North America                              │  0 │                                                             │
│ Eurasia                                    │  0 │                                                             │
│ Asia                                       │  0 │                                                             │
└────────────────────────────────────────────┴────┴─────────────────────────────────────────────────────────────┘
```

### regionToContinent

region を大陸 (geobase の type 1) に変換します。その他の点については、この関数は &#39;regionToCity&#39; と同じです。

**構文**

```sql
regionToContinent(id [, geobase])
```

**パラメータ**

* `id` — geobase 内の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases) を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 対応する大陸が存在する場合は、その Region ID。[UInt32](../data-types/int-uint)。
* 存在しない場合は 0。

**例**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                               │
│ World                                      │  0 │                                                               │
│ USA                                        │ 10 │ North America                                                 │
│ Colorado                                   │ 10 │ North America                                                 │
│ Boulder County                             │ 10 │ North America                                                 │
│ Boulder                                    │ 10 │ North America                                                 │
│ China                                      │ 12 │ Asia                                                          │
│ Sichuan                                    │ 12 │ Asia                                                          │
│ Chengdu                                    │ 12 │ Asia                                                          │
│ America                                    │  9 │ America                                                       │
│ North America                              │ 10 │ North America                                                 │
│ Eurasia                                    │ 11 │ Eurasia                                                       │
│ Asia                                       │ 12 │ Asia                                                          │
└────────────────────────────────────────────┴────┴───────────────────────────────────────────────────────────────┘
```

### regionToTopContinent

region の階層における最上位の大陸を返します。

**構文**

```sql
regionToTopContinent(id[, geobase])
```

**パラメーター**

* `id` — geobase の地域ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases) を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 地域の階層をさかのぼったときの最上位の大陸の識別子。[UInt32](../data-types/int-uint)。
* 存在しない場合は 0。

**例**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToTopContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                                  │
│ World                                      │  0 │                                                                  │
│ USA                                        │  9 │ America                                                          │
│ Colorado                                   │  9 │ America                                                          │
│ Boulder County                             │  9 │ America                                                          │
│ Boulder                                    │  9 │ America                                                          │
│ China                                      │ 11 │ Eurasia                                                          │
│ Sichuan                                    │ 11 │ Eurasia                                                          │
│ Chengdu                                    │ 11 │ Eurasia                                                          │
│ America                                    │  9 │ America                                                          │
│ North America                              │  9 │ America                                                          │
│ Eurasia                                    │ 11 │ Eurasia                                                          │
│ Asia                                       │ 11 │ Eurasia                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────────────┘
```

### regionToPopulation

地域の人口を取得します。人口情報は、geobase を含むファイルに記録できます。[&quot;Dictionaries&quot;](../statements/create/dictionary/embedded) のセクションを参照してください。地域の人口が記録されていない場合は、0 を返します。geobase では、人口が子地域には記録されていても、親地域には記録されていないことがあります。

**構文**

```sql
regionToPopulation(id[, geobase])
```

**パラメータ**

* `id` — geobase 内の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases) を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 地域の人口。[UInt32](../data-types/int-uint)。
* 該当するものがない場合は 0。

**例**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─population─┐
│                                            │          0 │
│ World                                      │ 4294967295 │
│ USA                                        │  330000000 │
│ Colorado                                   │    5700000 │
│ Boulder County                             │     330000 │
│ Boulder                                    │     100000 │
│ China                                      │ 1500000000 │
│ Sichuan                                    │   83000000 │
│ Chengdu                                    │   20000000 │
│ America                                    │ 1000000000 │
│ North America                              │  600000000 │
│ Eurasia                                    │ 4294967295 │
│ Asia                                       │ 4294967295 │
└────────────────────────────────────────────┴────────────┘
```

### regionIn

`lhs` の region が `rhs` の region に属しているかを判定します。属している場合は 1、属していない場合は 0 の UInt8 値を返します。

**構文**

```sql
regionIn(lhs, rhs\[, geobase\])
```

**パラメータ**

* `lhs` — geobase の lhs リージョン ID。[UInt32](../data-types/int-uint)。
* `rhs` — geobase の rhs リージョン ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 属している場合は 1。[UInt8](../data-types/int-uint)。
* 属していない場合は 0。

**実装の詳細**

この関係は反射的です。つまり、どのリージョンもそれ自身に属します。

**例**

```sql title="Query"
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

```text title="Response"
World is in World
World is not in USA
World is not in Colorado
World is not in Boulder County
World is not in Boulder
USA is in World
USA is in USA
USA is not in Colorado
USA is not in Boulder County
USA is not in Boulder    
```

### regionHierarchy

UInt32 の数値 (geobase の region ID) を受け取ります。指定した region と、親をたどった系統上のすべての親を含む region ID の配列を返します。

**構文**

```sql
regionHierarchy(id\[, geobase\])
```

**パラメータ**

* `id` — geobase の Region ID。[UInt32](../data-types/int-uint)。
* `geobase` — 辞書キー。[複数のジオベース](#multiple-geobases)を参照してください。[String](../data-types/string)。省略可能です。

**戻り値**

* 指定したリージョンと、親階層をたどったすべての親を含む Region ID の Array。[Array](../data-types/array)([UInt32](../data-types/int-uint))。

**例**

```sql title="Query"
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

```text title="Response"
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```

{/* 
  以下のタグ内の内容は、ドキュメントフレームワークのビルド時に
  system.functions から生成されたドキュメントに差し替えられます。タグは変更または削除しないでください。
  参照: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }