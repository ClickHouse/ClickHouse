---
description: '介绍 ClickHouse Cloud 中可用的 `Shared` 数据库引擎的页面。'
sidebar_label: 'Shared'
sidebar_position: 10
slug: /engines/database-engines/shared
title: 'Shared'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

<div id="shared-database-engine">
  # Shared 数据库引擎
</div>

`Shared` 数据库引擎与 Shared Catalog 配合使用，用于管理其表采用无状态表引擎 (例如 [`SharedMergeTree`](/zh/cloud/reference/shared-merge-tree)) 的数据库。
这些表引擎不会将持久状态写入磁盘，因此可兼容动态计算环境。

Cloud 中的 `Shared` 数据库引擎消除了对本地磁盘的依赖。
它是一个纯内存引擎，只需要 CPU 和内存。

<div id="how-it-works">
  ## 工作原理是什么？
</div>

`Shared` 数据库引擎将所有数据库和表定义存储在以 Keeper 为后端的中央 Shared Catalog 中。它不会写入本地磁盘，而是维护一个由所有计算节点共享的单一版本化全局状态。

每个节点只跟踪最后一次应用的版本，并在启动时拉取最新状态，无需本地文件或手动设置。

<div id="syntax">
  ## 语法
</div>

对于最终用户，使用 Shared Catalog 和 Shared 数据库引擎无需额外配置。创建数据库的方式与往常相同：

```sql
CREATE DATABASE my_database;
```

ClickHouse Cloud 会自动为数据库指定 Shared 数据库引擎。在这类数据库中，任何使用无状态引擎创建的表都会自动享有 Shared Catalog 的复制和协调能力。

:::tip
有关 Shared Catalog 及其优势的更多信息，请参阅 Cloud 参考部分中的[“Shared Catalog 和 Shared 数据库引擎”](/zh/cloud/reference/shared-catalog)。
:::