# 功能与Yandex的工作。梅特里卡词典 {#functions-for-working-with-yandex-metrica-dictionaries}

为了使下面的功能正常工作，服务器配置必须指定获取所有Yandex的路径和地址。梅特里卡字典. 字典在任何这些函数的第一次调用时加载。 如果无法加载引用列表，则会引发异常。

For information about creating reference lists, see the section «Dictionaries».

## 多个地理基 {#multiple-geobases}

ClickHouse支持同时使用多个备选地理基（区域层次结构），以支持某些地区所属国家的各种观点。

该 ‘clickhouse-server’ config指定具有区域层次结构的文件::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

除了这个文件，它还搜索附近有\_符号和任何后缀附加到名称（文件扩展名之前）的文件。
例如，它还会找到该文件 `/opt/geo/regions_hierarchy_ua.txt`，如果存在。

`ua` 被称为字典键。 对于没有后缀的字典，键是空字符串。

所有字典都在运行时重新加载（每隔一定数量的秒重新加载一次，如builtin\_dictionaries\_reload\_interval config参数中定义，或默认情况下每小时一次）。 但是，可用字典列表在服务器启动时定义一次。

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
示例:

    regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
    regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
    regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt

### ﾂ环板(ｮﾂ嘉ｯﾂ偲青regionｼﾂ氾ｶﾂ鉄ﾂ工ﾂ渉\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### 虏茅驴麓卤戮碌禄路戮鲁拢\]) {#regiontoareaid-geobase}

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

### 虏茅驴麓卤戮碌禄路戮鲁拢(陆毛隆隆(803)888-8325\]) {#regiontocountryid-geobase}

将区域转换为国家。 在所有其他方式，这个功能是一样的 ‘regionToCity’.
示例: `regionToCountry(toUInt32(213)) = 225` 转换莫斯科（213）到俄罗斯（225）。

### 掳胫((禄脢鹿脷露胫鲁隆鹿((酶-11-16""\[脪陆,ase\]) {#regiontocontinentid-geobase}

将区域转换为大陆。 在所有其他方式，这个功能是一样的 ‘regionToCity’.
示例: `regionToContinent(toUInt32(213)) = 10001` 将莫斯科（213）转换为欧亚大陆（10001）。

### ﾂ环板(ｮﾂ嘉ｯﾂ偲青regionｬﾂ静ｬﾂ青ｻﾂ催ｬﾂ渉\]) {#regiontopopulationid-geobase}

获取区域的人口。
The population can be recorded in files with the geobase. See the section «External dictionaries».
如果没有为该区域记录人口，则返回0。
在Yandex地理数据库中，可能会为子区域记录人口，但不会为父区域记录人口。

### regionIn(lhs,rhs\[,地理数据库\]) {#regioninlhs-rhs-geobase}

检查是否 ‘lhs’ 属于一个区域 ‘rhs’ 区域。 如果属于UInt8，则返回等于1的数字，如果不属于则返回0。
The relationship is reflexive – any region also belongs to itself.

### ﾂ暗ｪﾂ氾环催ﾂ団ﾂ法ﾂ人\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
示例: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### 地区名称(id\[,郎\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn’t exist, an empty string is returned.

`ua` 和 `uk` 都意味着乌克兰。

[原始文章](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
