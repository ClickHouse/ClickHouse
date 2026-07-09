---
description: '用于操作嵌入式字典的函数文档'
sidebar_label: '嵌入式字典'
slug: /sql-reference/functions/ym-dict-functions
title: '用于操作嵌入式字典的函数'
doc_type: 'reference'
---

:::note
要使下面的函数正常工作，必须在服务器配置中指定获取所有嵌入式字典所需的路径和地址。这些字典会在首次调用其中任一函数时加载。如果无法加载参考列表，则会引发异常。

因此，除非事先完成配置，否则本节所示示例默认会在 [ClickHouse Fiddle](https://fiddle.clickhouse.com/) 以及 quick release 和生产部署中引发异常。
:::

有关创建参考列表的信息，请参见“[字典](../statements/create/dictionary/embedded)”部分。

<div id="multiple-geobases">
  ## 多个地理库
</div>

ClickHouse 支持同时使用多个可选的 geobase (区域层级) ，以便支持从不同视角判断某些区域归属哪个国家。

`clickhouse-server` 配置指定了包含区域层级的文件：

`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

除了这个文件外，它还会查找附近名称中在文件扩展名前附加了 `_` 符号和任意后缀的文件。
例如，如果存在，它也会找到文件 `/opt/geo/regions_hierarchy_ua.txt`。其中，`ua` 被称为字典键。对于没有后缀的字典，键为空字符串。

所有字典都会在运行时重新加载 (按 [`builtin_dictionaries_reload_interval`](/zh/operations/server-configuration-parameters/settings#builtin_dictionaries_reload_interval) 配置参数定义的间隔每隔若干秒一次，默认则为每小时一次) 。不过，可用字典的列表只会在服务器启动时确定一次。

所有处理区域的函数在末尾都有一个可选参数——字典键。它被称为 geobase。

示例：

```sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

接受区域 ID 和 geobase 作为输入，返回对应语言的该区域名称字符串。如果指定 ID 的区域不存在，则返回空字符串。

**语法**

```sql
regionToName(id\[, lang\])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 由 `geobase` 指定的对应语言的区域名称。[String](../data-types/string)。
* 否则，返回空字符串。

**示例**

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

接受 geobase 中的区域 ID。如果该区域是城市或城市的一部分，则返回对应城市的区域 ID。否则返回 0。

**语法**

```sql
regionToCity(id [, geobase])
```

**参数**

* `id` — 来自 geobase 的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见 [多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 对应城市的区域 ID (如果存在) 。[UInt32](../data-types/int-uint)。
* 如果不存在，则返回 0。

**示例**

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

将 region 转换为 area (geobase 中的类型 5) 。除此之外，此函数与 [&#39;regionToCity&#39;](#regiontocity) 相同。

**语法**

```sql
regionToArea(id [, geobase])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 对应区域的区域 ID (如果存在) 。[UInt32](../data-types/int-uint)。
* 如果不存在，则为 0。

**示例**

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

将地区转换为联邦区 (geobase 中的类型 4) 。在其他方面，此函数与 &#39;regionToCity&#39; 相同。

**语法**

```sql
regionToDistrict(id [, geobase])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 如果存在，返回对应城市的区域 ID。[UInt32](../data-types/int-uint)。
* 如果不存在，返回 0。

**示例**

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

将区域转换为国家 (geobase 中的类型 3) 。除此之外，该函数与 &#39;regionToCity&#39; 相同。

**语法**

```sql
regionToCountry(id [, geobase])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。请参阅[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 如果存在，则返回对应国家的区域 ID。[UInt32](../data-types/int-uint)。
* 如果不存在，则返回 0。

**示例**

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

将地区映射到大洲 (geobase 中的类型 1) 。除此之外，此函数在其他方面与 &#39;regionToCity&#39; 相同。

**语法**

```sql
regionToContinent(id [, geobase])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 对应大洲的区域 ID (如果存在) 。[UInt32](../data-types/int-uint)。
* 如果不存在，则返回 0。

**示例**

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

查找该区域在层级结构中对应的最高级大洲。

**语法**

```sql
regionToTopContinent(id[, geobase])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 顶层大洲的标识符 (即沿区域层级向上追溯后得到的大洲) 。[UInt32](../data-types/int-uint)。
* 如果不存在，则为 0。

**示例**

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

获取某个区域的人口数。人口信息可以记录在包含 geobase 的文件中。请参见[“字典”](../statements/create/dictionary/embedded)一节。如果该区域未记录人口数，则返回 0。在 geobase 中，人口数可能记录在下级区域中，但不会记录在上级区域中。

**语法**

```sql
regionToPopulation(id[, geobase])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 该区域的人口。[UInt32](../data-types/int-uint)。
* 如果不存在，则为 0。

**示例**

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

检查 `lhs` 区域是否隶属于 `rhs` 区域。若隶属于，则返回值为 1 的 UInt8 数值；若不隶属于，则返回 0。

**语法**

```sql
regionIn(lhs, rhs\[, geobase\])
```

**参数**

* `lhs` — geobase 中的 lhs 区域 ID。[UInt32](../data-types/int-uint)。
* `rhs` — geobase 中的 rhs 区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。请参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 如果属于，返回 1。[UInt8](../data-types/int-uint)。
* 如果不属于，返回 0。

**实现细节**

该关系具有自反性——任何区域都属于其自身。

**示例**

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

接受一个 UInt32 数值，即 geobase 中的区域 ID。返回一个区域 ID 数组，包含传入的区域及其沿父级链向上的所有父级区域 ID。

**语法**

```sql
regionHierarchy(id\[, geobase\])
```

**参数**

* `id` — geobase 中的区域 ID。[UInt32](../data-types/int-uint)。
* `geobase` — 字典键。参见[多个地理库](#multiple-geobases)。[String](../data-types/string)。可选。

**返回值**

* 由传入区域及其沿父级链上的所有父区域的区域 ID 组成的数组。[Array](../data-types/array)([UInt32](../data-types/int-uint))。

**示例**

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
  以下标签内的内容会在文档框架构建时替换为
  根据 system.functions 生成的文档。请勿修改或删除这些标签。
  参见：https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }