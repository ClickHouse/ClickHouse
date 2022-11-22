---
sidebar_position: 59
sidebar_label: Embedded Dictionaries
---

# Functions for Working with Embedded Dictionaries

In order for the functions below to work, the server config must specify the paths and addresses for getting all the embedded dictionaries. The dictionaries are loaded at the first call of any of these functions. If the reference lists can’t be loaded, an exception is thrown.

For information about creating reference lists, see the section “Dictionaries”.

## Multiple Geobases

ClickHouse supports working with multiple alternative geobases (regional hierarchies) simultaneously, in order to support various perspectives on which countries certain regions belong to.

The ‘clickhouse-server’ config specifies the file with the regional hierarchy::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

Besides this file, it also searches for files nearby that have the _ symbol and any suffix appended to the name (before the file extension).
For example, it will also find the file `/opt/geo/regions_hierarchy_ua.txt`, if present.

`ua` is called the dictionary key. For a dictionary without a suffix, the key is an empty string.

All the dictionaries are re-loaded in runtime (once every certain number of seconds, as defined in the builtin_dictionaries_reload_interval config parameter, or once an hour by default). However, the list of available dictionaries is defined one time, when the server starts.

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
Example:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToCity(id\[, geobase\])

Accepts a UInt32 number – the region ID from the geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### regionToArea(id\[, geobase\])

Converts a region to an area (type 5 in the geobase). In every other way, this function is the same as ‘regionToCity’.

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

### regionToDistrict(id\[, geobase\])

Converts a region to a federal district (type 4 in the geobase). In every other way, this function is the same as ‘regionToCity’.

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

### regionToCountry(id\[, geobase\])

Converts a region to a country. In every other way, this function is the same as ‘regionToCity’.
Example: `regionToCountry(toUInt32(213)) = 225` converts Moscow (213) to Russia (225).

### regionToContinent(id\[, geobase\])

Converts a region to a continent. In every other way, this function is the same as ‘regionToCity’.
Example: `regionToContinent(toUInt32(213)) = 10001` converts Moscow (213) to Eurasia (10001).

### regionToTopContinent(id\[, geobase\])

Finds the highest continent in the hierarchy for the region.

**Syntax**

``` sql
regionToTopContinent(id[, geobase])
```

**Arguments**

-   `id` — Region ID from the geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). Optional.

**Returned value**

-   Identifier of the top level continent (the latter when you climb the hierarchy of regions).
-   0, if there is none.

Type: `UInt32`.

### regionToPopulation(id\[, geobase\])

Gets the population for a region.
The population can be recorded in files with the geobase. See the section “External dictionaries”.
If the population is not recorded for the region, it returns 0.
In the geobase, the population might be recorded for child regions, but not for parent regions.

### regionIn(lhs, rhs\[, geobase\])

Checks whether a ‘lhs’ region belongs to a ‘rhs’ region. Returns a UInt8 number equal to 1 if it belongs, or 0 if it does not belong.
The relationship is reflexive – any region also belongs to itself.

### regionHierarchy(id\[, geobase\])

Accepts a UInt32 number – the region ID from the geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
Example: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### regionToName(id\[, lang\])

Accepts a UInt32 number – the region ID from the geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID does not exist, an empty string is returned.

`ua` and `uk` both mean Ukrainian.

