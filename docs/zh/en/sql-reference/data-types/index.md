---
description: 'ClickHouse 数据类型文档'
sidebar_label: '数据类型列表'
sidebar_position: 1
slug: /sql-reference/data-types/
title: 'ClickHouse 中的数据类型'
doc_type: 'reference'
---

本节介绍 ClickHouse 支持的数据类型，例如 [integers](int-uint.md)、[floats](float.md) 和 [strings](string.md)。

系统表 [system.data&#95;type&#95;families](/zh/operations/system-tables/data_type_families) 提供了
所有可用数据类型的概览。
它还会显示某种数据类型是否为另一种数据类型的别名，以及其名称是否区分大小写 (例如 `bool` 与 `BOOL`) 。