---
description: '介绍 ClickHouse 对第三方库的使用以及如何添加和维护
  第三方库的页面。'
sidebar_label: '第三方库'
sidebar_position: 60
slug: /development/contrib
title: '第三方库'
doc_type: 'reference'
---

ClickHouse 出于不同目的使用第三方库，例如连接其他数据库、在从磁盘加载或保存数据时进行解码/编码，或实现某些专用的 SQL 函数。
为避免依赖目标系统中已有的库，每个第三方库都会作为 Git submodule 导入 ClickHouse 的源代码树中，并与 ClickHouse 一同编译和链接。
第三方库及其许可证列表可通过以下查询获取：

```sql
SELECT library_name, license_type, license_path FROM system.licenses ORDER BY library_name COLLATE 'en';
```

请注意，这里列出的库都是位于 ClickHouse 仓库 `contrib/` 目录中的库。
根据构建选项的不同，其中一些库可能未被编译，因此其功能在运行时可能不可用。

[示例](https://sql.clickhouse.com?query_id=478GCPU7LRTSZJBNY3EJT3)

<div id="adding-and-maintaining-third-party-libraries">
  ## 添加和维护第三方库
</div>

每个第三方库都必须放在 ClickHouse 仓库的 `contrib/` 目录下的独立目录中。
不要把外部代码的副本直接丢进库目录。
应创建一个 Git submodule，从外部上游仓库拉取第三方代码。

ClickHouse 使用的所有 submodule 都列在 `.gitmodule` 文件中。

* 如果该库可以直接使用 (默认情况) ，可以直接引用上游仓库。
* 如果该库需要打补丁，请在 [GitHub 上的 ClickHouse organization](https://github.com/ClickHouse) 中为该上游仓库创建一个 fork。

在后一种情况下，我们的目标是尽可能将自定义补丁与上游提交隔离开。
为此，请从你想集成的分支或标签创建一个带 `ClickHouse/` 前缀的分支，例如 `ClickHouse/2024_2` (对应分支 `2024_2`) 或 `ClickHouse/release/vX.Y.Z` (对应标签 `release/vX.Y.Z`) 。
避免跟踪上游开发分支 `master`/ `main` / `dev` (也就是不要在 fork 仓库中使用前缀分支 `ClickHouse/master` / `ClickHouse/main` / `ClickHouse/dev`) 。
这类分支是不断变化的目标，会让正确的版本管理更加困难。
这种“前缀分支”可以确保把上游仓库的更新拉到 fork 中时，不会影响自定义的 `ClickHouse/` 分支。
`contrib/` 中的 submodule 只能跟踪已 fork 的第三方仓库里的 `ClickHouse/` 分支。

补丁只能应用到外部库的 `ClickHouse/` 分支上。

有两种做法：

* 如果你想针对 fork 仓库中的某个 `ClickHouse/` 前缀分支制作新的修复，例如 sanitizer 修复，那么请将该修复以带 `ClickHouse/` 前缀的分支形式推送，例如 `ClickHouse/fix-sanitizer-disaster`。然后从这个新分支向自定义跟踪分支发起 PR，例如 `ClickHouse/2024_2 <-- ClickHouse/fix-sanitizer-disaster`，并合并该 PR。
* 如果你是在更新 submodule，并且需要重新应用之前的补丁，那么没必要重建旧 PR。此时，直接将旧的提交 cherry-pick 到新的 `ClickHouse/` 分支中即可 (对应新版本) 。如果某个 PR 包含多个提交，可以酌情将这些提交 squash。最理想的情况是，我们已经把自定义补丁贡献回上游，因此在新版本中就可以省掉这些补丁。

submodule 更新完成后，请在 ClickHouse 中更新该 submodule，使其指向 fork 中的新哈希值。

为第三方库创建补丁时，请始终以官方仓库为参照，并考虑将补丁贡献回上游仓库。
这样既能确保其他人也能从补丁中受益，也不会给 ClickHouse 团队带来额外的维护负担。