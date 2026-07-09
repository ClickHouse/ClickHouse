---
description: 'ClickHouse 内置的 geobase 字典'
sidebar_label: '嵌入式字典'
sidebar_position: 6
slug: /sql-reference/statements/create/dictionary/embedded
title: '嵌入式（geobase）字典'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse 内置了处理 geobase 的功能。

这使您可以：

* 使用某个区域的 ID 获取其在指定语言中的名称。
* 使用某个区域的 ID 获取城市、地区、联邦区、国家或大洲的 ID。
* 检查某个区域是否属于另一个区域。
* 获取父级区域链。

所有函数都支持“translocality”，即能够同时采用关于区域归属的不同视角。更多信息，请参见“用于处理网站分析字典的函数”一节。

默认软件包中禁用了内部字典。
要启用它们，请在服务器配置文件中取消注释参数 `path_to_regions_hierarchy_file` 和 `path_to_regions_names_files`。

geobase 从文本文件中加载。

将 `regions_hierarchy*.txt` 文件放入 `path_to_regions_hierarchy_file` 目录中。该配置参数必须包含 `regions_hierarchy.txt` 文件 (默认区域层级) 的路径，其他文件 (`regions_hierarchy_ua.txt`) 也必须位于同一目录中。

将 `regions_names_*.txt` 文件放入 `path_to_regions_names_files` 目录中。

您也可以自行创建这些文件。文件格式如下：

`regions_hierarchy*.txt`：TabSeparated (无表头) ，列：

* 区域 ID (`UInt32`)
* 父区域 ID (`UInt32`)
* 区域类型 (`UInt8`)：1 - 大洲，3 - 国家，4 - 联邦区，5 - 区域，6 - 城市；其他类型没有值
* 人口 (`UInt32`) — 可选列

`regions_names_*.txt`：TabSeparated (无表头) ，列：

* 区域 ID (`UInt32`)
* 区域名称 (`String`) — 不能包含制表符或换行符，即使是转义后的也不行。

在 RAM 中存储时使用扁平 Array。因此，ID 不应超过一百万。

字典可以在不重启服务器的情况下更新。不过，可用字典的集合不会更新。
更新时，会检查文件的修改时间。如果某个文件发生变化，则会更新对应的字典。
检查变更的时间间隔通过 `builtin_dictionaries_reload_interval` 参数配置。
字典更新 (首次使用时的加载除外) 不会阻塞查询。在更新期间，查询会使用旧版本的字典。如果更新过程中发生错误，错误会写入服务器日志，查询将继续使用旧版本的字典。

我们建议定期用 geobase 更新字典。更新时，请在单独的位置生成新文件并写入该位置。待一切准备就绪后，再将其重命名为服务器正在使用的文件。

还有一些用于处理 OS 标识符和搜索引擎的函数，但不应使用它们。