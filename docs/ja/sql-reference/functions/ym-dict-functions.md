---
slug: /ja/sql-reference/functions/ym-dict-functions
sidebar_position: 60
sidebar_label: 組み込みDictionary
---

# 組み込みDictionaryを扱うための関数

:::note
以下の関数が動作するには、サーバーの設定で組み込みDictionaryを取得するためのパスとアドレスを指定する必要があります。これらの関数が最初に呼び出された際にDictionaryが読み込まれます。参照リストを読み込めない場合は例外が投げられます。

したがって、このセクションで示す例は、[ClickHouse Fiddle](https://fiddle.clickhouse.com/)や、クイックリリースやプロダクションのデフォルト設定では例外が発生しますが、事前に設定された場合を除きます。
:::

参照リストの作成に関する情報は、[「Dictionary」](../dictionaries#embedded-dictionaries)のセクションを参照してください。

## 複数のジオベース

ClickHouseは、ある地域がどの国に属するかについて様々な視点をサポートするために、複数の代替ジオベース（地域階層）を同時に扱うことができます。

'clickhouse-server' の設定で地域階層を持つファイルを指定します。

```<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>```

このファイルに加えて、ファイル名に `_` 記号と任意のサフィックスが付けられた、拡張子の前に配置された近くのファイルも検索されます。例えば、`/opt/geo/regions_hierarchy_ua.txt` というファイルが存在する場合、そのファイルも見つけられます。ここで `ua` はDictionaryキーと呼ばれます。サフィックスのないDictionaryについては、キーは空文字列になります。

すべてのDictionaryはランタイム中に再読み込みされます（[`builtin_dictionaries_reload_interval`](../../operations/server-configuration-parameters/settings#builtin-dictionaries-reload-interval) 設定パラメータで定義された秒数またはデフォルトで1時間に一度）。ただし、利用可能なDictionaryのリストはサーバーが開始された時点で一度定義されます。

地域を扱うすべての関数には、最後にDictionaryキーとして参照されるジオベースというオプションの引数があります。

例:

``` sql
regionToCountry(RegionID) – デフォルトDictionaryを使用: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – デフォルトDictionaryを使用: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – 'ua' キーのDictionaryを使用: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

地域IDとジオベースを受け取り、対応する言語で地域の名称を返します。指定されたIDの地域が存在しない場合、空の文字列が返されます。

**構文**

``` sql
regionToName(id\[, lang\])
```
**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- `geobase` で指定された言語の地域名。[String](../data-types/string).
- それ以外の場合は空の文字列。

**例**

クエリ:

``` sql
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

結果:

``` text
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```

### regionToCity

ジオベースから地域IDを受け取ります。この地域が都市または都市の一部である場合、適切な都市の地域IDを返します。それ以外の場合は0を返します。

**構文**

```sql
regionToCity(id [, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 適切な都市の地域IDがあれば。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

```sql
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

結果:

```response
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

地域をエリア（ジオベース内のタイプ5）に変換します。この関数は、[‘regionToCity’](#regiontocity)と同様です。

**構文**

```sql
regionToArea(id [, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 適切なエリアの地域IDがあれば。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

結果:

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

### regionToDistrict

地域を連邦区（ジオベース内のタイプ4）に変換します。この関数は、‘regionToCity’と同様です。

**構文**

```sql
regionToDistrict(id [, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 適切な都市の地域IDがあれば。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

結果:

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

### regionToCountry

地域を国（ジオベース内のタイプ3）に変換します。この関数は、‘regionToCity’と同様です。

**構文**

```sql
regionToCountry(id [, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 適切な国の地域IDがあれば。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

結果:

``` text
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

地域を大陸（ジオベース内のタイプ1）に変換します。この関数は、‘regionToCity’と同様です。

**構文**

```sql
regionToContinent(id [, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 適切な大陸の地域IDがあれば。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

結果:

``` text
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

地域の階層で最上位の大陸を見つけます。

**構文**

``` sql
regionToTopContinent(id[, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 領域の階層を登ったときの最上位の大陸の識別子。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

結果:

``` text
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

地域の人口を取得します。人口はジオベースのファイルに記録されていることがあります。[「Dictionary」](../dictionaries#embedded-dictionaries)セクションを参照してください。地域の人口が記録されていない場合は0を返します。ジオベースでは、子地域の人口は記録されているが親地域には記録されていないことがあります。

**構文**

``` sql
regionToPopulation(id[, geobase])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 地域の人口。[UInt32](../data-types/int-uint).
- そうでなければ0。

**例**

クエリ:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

結果:

``` text
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

`lhs` 地域が `rhs` 地域に属するかどうかを確認します。属する場合は1、属さない場合は0のUInt8値を返します。

**構文**

``` sql
regionIn(lhs, rhs\[, geobase\])
```

**パラメータ**

- `lhs` — ジオベースのlhs地域ID。[UInt32](../data-types/int-uint).
- `rhs` — ジオベースのrhs地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 属する場合は1。[UInt8](../data-types/int-uint).
- 属さない場合は0。

**実装の詳細**

この関係は反射的です。すなわち、どの地域も自分自身に属します。

**例**

クエリ:

``` sql
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

結果:

``` text
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

ジオベースの地域IDであるUInt32数値を受け取ります。渡された地域とその沿線のすべての親地域からなる地域IDの配列を返します。

**構文**

``` sql
regionHierarchy(id\[, geobase\])
```

**パラメータ**

- `id` — ジオベースの地域ID。[UInt32](../data-types/int-uint).
- `geobase` — Dictionaryキー。[複数のジオベース](#multiple-geobases)を参照。[String](../data-types/string). オプション。

**返される値**

- 渡された地域とその沿線のすべての親地域からなる地域IDの配列。[Array](../data-types/array)([UInt32](../data-types/int-uint)).

**例**

クエリ:

``` sql
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

結果:

``` text
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```
