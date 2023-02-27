# 使用 Yandex.Metrica 字典函数 {#functions-for-working-with-yandex-metrica-dictionaries}

为了使下面的功能正常工作，服务器配置必须指定获取所有 Yandex.Metrica 字典的路径和地址。Yandex.Metrica 字典在任何这些函数的第一次调用时加载。 如果无法加载引用列表，则会引发异常。

有关创建引用列表的信息，请参阅 «字典» 部分.

## 多个地理基 {#multiple-geobases}

ClickHouse支持同时使用多个备选地理基（区域层次结构），以支持某些地区所属国家的各种观点。

该 ‘clickhouse-server’ config指定具有区域层次结构的文件::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

除了这个文件，它还搜索附近有_符号和任何后缀附加到名称（文件扩展名之前）的文件。
例如，它还会找到该文件 `/opt/geo/regions_hierarchy_ua.txt`，如果存在。

`ua` 被称为字典键。 对于没有后缀的字典，键是空字符串。

所有字典都在运行时重新加载（每隔一定数量的秒重新加载一次，如builtin_dictionaries_reload_interval config参数中定义，或默认情况下每小时一次）。 但是，可用字典列表在服务器启动时定义一次。

所有处理区域的函数都在末尾有一个可选参数—字典键。它被称为地基。
示例:

    regionToCountry(RegionID) – 使用默认路径: /opt/geo/regions_hierarchy.txt
    regionToCountry(RegionID, '') – 使用默认路径: /opt/geo/regions_hierarchy.txt
    regionToCountry(RegionID, 'ua') – 使用字典中的'ua' 键: /opt/geo/regions_hierarchy_ua.txt

### regionToCity(id[, geobase]) {#regiontocityid-geobase}

从 Yandex geobase 接收一个 UInt32 数字类型的区域ID 。如果该区域是一个城市或城市的一部分，它将返回相应城市的区域ID。否则,返回0。

### regionToArea(id[, geobase]) {#regiontoareaid-geobase}

将区域转换为区域（地理数据库中的类型5）。 在所有其他方式，这个功能是一样的 ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

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

### regionToDistrict(id\[,geobase\]) {#regiontodistrictid-geobase}

将区域转换为联邦区（地理数据库中的类型4）。 在所有其他方式，这个功能是一样的 ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

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

### regionToCountry(id[, geobase]) {#regiontocountryid-geobase}

将区域转换为国家。 在所有其他方式，这个功能是一样的 ‘regionToCity’.
示例: `regionToCountry(toUInt32(213)) = 225` 转换莫斯科（213）到俄罗斯（225）。

### regionToContinent(id[, geobase]) {#regiontocontinentid-geobase}

将区域转换为大陆。 在所有其他方式，这个功能是一样的 ‘regionToCity’.
示例: `regionToContinent(toUInt32(213)) = 10001` 将莫斯科（213）转换为欧亚大陆（10001）。

### regionToTopContinent (#regiontotopcontinent) {#regiontotopcontinent-regiontotopcontinent}

查找该区域层次结构中最高的大陆。

**语法**

``` sql
regionToTopContinent(id[, geobase])
```

**参数**

-   `id` — Yandex geobase 的区域 ID. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — 字典的建. 参阅 [Multiple Geobases](#multiple-geobases). [String](../../sql-reference/data-types/string.md). 可选.

**返回值**

-   顶级大陆的标识符(当您在区域层次结构中攀爬时，是后者)。
-   0，如果没有。

类型: `UInt32`.

### regionToPopulation(id\[, geobase\]) {#regiontopopulationid-geobase}

获取区域的人口。
人口可以记录在文件与地球基。请参阅«外部词典»部分。
如果没有为该区域记录人口，则返回0。
在Yandex地理数据库中，可能会为子区域记录人口，但不会为父区域记录人口。

### regionIn(lhs,rhs\[,地理数据库\]) {#regioninlhs-rhs-geobase}

检查是否 ‘lhs’ 属于一个区域 ‘rhs’ 区域。 如果属于UInt8，则返回等于1的数字，如果不属于则返回0。
这种关系是反射的——任何地区也属于自己。

### regionHierarchy(id\[, geobase\]) {#regionhierarchyid-geobase}

从 Yandex geobase 接收一个 UInt32 数字类型的区域ID。返回一个区域ID数组，由传递的区域和链上的所有父节点组成。
示例: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### regionToName(id\[, lang\]) {#regiontonameid-lang}

从 Yandex geobase 接收一个 UInt32 数字类型的区域ID。带有语言名称的字符串可以作为第二个参数传递。支持的语言有:ru, en, ua, uk, by, kz, tr。如果省略第二个参数，则使用' ru '语言。如果不支持该语言，则抛出异常。返回一个字符串-对应语言的区域名称。如果指定ID的区域不存在，则返回一个空字符串。

`ua` 和 `uk` 都意味着乌克兰。

[原始文章](https://clickhouse.com/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
