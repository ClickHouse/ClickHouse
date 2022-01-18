---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u5185\u90E8\u5B57\u5178"
---

# 内部字典 {#internal_dicts}

ClickHouse包含用于处理地理数据库的内置功能。

这使您可以:

-   使用区域的ID以所需语言获取其名称。
-   使用区域ID获取城市、地区、联邦区、国家或大陆的ID。
-   检查一个区域是否属于另一个区域。
-   获取父区域链。

所有功能支持 “translocality,” 能够同时使用不同的角度对区域所有权。 有关详细信息，请参阅部分 “Functions for working with Yandex.Metrica dictionaries”.

在默认包中禁用内部字典。
要启用它们，请取消注释参数 `path_to_regions_hierarchy_file` 和 `path_to_regions_names_files` 在服务器配置文件中。

Geobase从文本文件加载。

将 `regions_hierarchy*.txt` 文件到 `path_to_regions_hierarchy_file` 目录。 此配置参数必须包含指向 `regions_hierarchy.txt` 文件（默认区域层次结构）和其他文件 (`regions_hierarchy_ua.txt`）必须位于同一目录中。

把 `regions_names_*.txt` 在文件 `path_to_regions_names_files` 目录。

您也可以自己创建这些文件。 文件格式如下:

`regions_hierarchy*.txt`：TabSeparated（无标题），列:

-   地区ID (`UInt32`)
-   父区域ID (`UInt32`)
-   区域类型 (`UInt8`）：1-大陆，3-国家，4-联邦区，5-地区，6-城市;其他类型没有价值
-   人口 (`UInt32`) — optional column

`regions_names_*.txt`：TabSeparated（无标题），列:

-   地区ID (`UInt32`)
-   地区名称 (`String`) — Can't contain tabs or line feeds, even escaped ones.

平面阵列用于存储在RAM中。 出于这个原因，Id不应该超过一百万。

字典可以在不重新启动服务器的情况下更新。 但是，不会更新可用字典集。
对于更新，将检查文件修改时间。 如果文件已更改，则更新字典。
检查更改的时间间隔在 `builtin_dictionaries_reload_interval` 参数。
字典更新（首次使用时加载除外）不会阻止查询。 在更新期间，查询使用旧版本的字典。 如果在更新过程中发生错误，则将错误写入服务器日志，并使用旧版本的字典继续查询。

我们建议定期使用geobase更新字典。 在更新期间，生成新文件并将其写入单独的位置。 一切准备就绪后，将其重命名为服务器使用的文件。

还有与操作系统标识符和Yandex的工作功能。Metrica搜索引擎，但他们不应该被使用。

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/internal_dicts/) <!--hide-->
