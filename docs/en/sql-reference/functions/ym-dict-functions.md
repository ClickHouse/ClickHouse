---
slug: /en/sql-reference/functions/ym-dict-functions
sidebar_position: 60
sidebar_label: Embedded Dictionaries
---

# Functions for Working with Embedded Dictionaries

:::note
In order for the functions below to work, the server config must specify the paths and addresses for getting all the embedded dictionaries. The dictionaries are loaded at the first call of any of these functions. If the reference lists can’t be loaded, an exception is thrown.

As such, the examples shown in this section cannot be run in [ClickHouse Fiddle](https://fiddle.clickhouse.com/).
:::

For information about creating reference lists, see the section [“Dictionaries”](../../sql-reference/dictionaries/index.md).

## Multiple Geobases

ClickHouse supports working with multiple alternative geobases (regional hierarchies) simultaneously, in order to support various perspectives on which countries certain regions belong to.

The ‘clickhouse-server’ config specifies the file with the regional hierarchy::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Besides this file, it also searches for files nearby that have the _ symbol and any suffix appended to the name (before the file extension).
For example, it will also find the file `/opt/geo/regions_hierarchy_ua.txt`, if present.

`ua` is called the dictionary key. For a dictionary without a suffix, the key is an empty string.

All the dictionaries are re-loaded in runtime (once every certain number of seconds, as defined in the [`builtin_dictionaries_reload_interval`](../../operations/server-configuration-parameters/settings#builtin-dictionaries-reload-interval) config parameter, or once an hour by default). However, the list of available dictionaries is defined one time, when the server starts.

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.

Example:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToCity

Accepts a region ID from the geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

**Syntax**

```sql
regionToCity(id [, geobase])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Region ID for the appropriate city, if it exists.
- 0, if there is none.

Type: `UInt32`.

**Example**

Query:

```sql
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

Result:

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

Converts a region to an area (type 5 in the geobase). In every other way, this function is the same as [‘regionToCity’](#regiontocity).

**Syntax**

```sql
regionToArea(id [, geobase])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Area
- 0, if there is none.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

Result:

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

Converts a region to a federal district (type 4 in the geobase). In every other way, this function is the same as ‘regionToCity’.

**Syntax**

```sql
regionToDistrict(id [, geobase])
```

**Parameters**

- `id`: the region ID from the geobase. [UInt32](../data-types/int-uint.md/#uint8-uint16-uint32-uint64-uint128-uint256-int8-int16-int32-int64-int128-int256)
- `geobase` (optional): geobase (dictionary key). [Dictionary Key](../../sql-reference/dictionaries/index.md/#dictionary-key-and-fields-dictionary-key-and-fields)

**Returned value**

- Region ID for the appropriate city, if it exists.
- 0, if there is none.

Type: `UInt32`. 

**Example**

Query:

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

Result:

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

Converts a region to a country. In every other way, this function is the same as ‘regionToCity’.
Example: `regionToCountry(toUInt32(213)) = 225` converts Moscow (213) to Russia (225).

**Syntax**

```sql
regionToCountry(id [, geobase])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Region ID for the appropriate country, if it exists.
- 0, if there is none.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

Result:

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

Converts a region to a continent. In every other way, this function is the same as ‘regionToCity’.

**Syntax**

```sql
regionToContinent(id [, geobase])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Region ID for the appropriate continent, if it exists.
- 0, if there is none.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

Result:

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

Finds the highest continent in the hierarchy for the region.

**Syntax**

``` sql
regionToTopContinent(id[, geobase])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Identifier of the top level continent (the latter when you climb the hierarchy of regions).
- 0, if there is none.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

Result:

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

Gets the population for a region. The population can be recorded in files with the geobase. See the section ["Dictionaries"](../../sql-reference/dictionaries/index.md). If the population is not recorded for the region, it returns 0. In the geobase, the population might be recorded for child regions, but not for parent regions.

**Syntax**

``` sql
regionToPopulation(id[, geobase])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Population for the region.
- 0, if there is none.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

Result:

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

Checks whether a ‘lhs’ region belongs to a ‘rhs’ region. Returns a UInt8 number equal to 1 if it belongs, or 0 if it does not belong.

**Syntax**

``` sql
regionIn(lhs, rhs\[, geobase\])
```

**Parameters**

- `lhs` — Lhs region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `rhs` — Rhs region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- 1, if it belongs.
- 0, if it doesn't belong.

Type: `UInt8`.

**Implementation details**

The relationship is reflexive – any region also belongs to itself.

**Example**

Query:

``` sql
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

Result:

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

Accepts a UInt32 number – the region ID from the geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
Example: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

**Syntax**

``` sql
regionHierarchy(id\[, geobase\])
```

**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Array of region IDs consisting of the passed region and all parents along the chain.

Type: `Array(UInt32)`


**Example**

Query:

``` sql
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

Result:

``` text
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```

### regionToName

Accepts a region ID and geobase and returns a string of the name of the region in the corresponding language. If the region with the specified ID does not exist, an empty string is returned.

**Syntax**

``` sql
regionToName(id\[, lang\])
```
**Parameters**

- `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
- `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

- Name of the region in the corresponding language specified by `geobase`. 

Type: String.

**Example**

Query:

``` sql
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

Result:

``` text
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```