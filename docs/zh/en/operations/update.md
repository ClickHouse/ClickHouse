---
description: '升级文档'
sidebar_title: '自管理升级'
slug: /operations/update
title: '自管理升级'
doc_type: 'guide'
---

<div id="clickhouse-upgrade-overview">
  ## ClickHouse 升级概述
</div>

本文档包含：

* 一般指导原则
* 推荐方案
* 在您的系统上升级二进制文件的具体说明

<div id="general-guidelines">
  ## 一般指南
</div>

以下说明可帮助你做好规划，并理解我们为何会在本文档后文提出这些建议。

<div id="upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper">
  ### 将 ClickHouse server 与 ClickHouse Keeper 或 ZooKeeper 分开升级
</div>

除非 ClickHouse Keeper 或 Apache ZooKeeper 需要安全修复，否则在升级 ClickHouse server 时无需同时升级 Keeper。升级过程中必须保持 Keeper 的稳定性，因此应先完成 ClickHouse server 的升级，再考虑升级 Keeper。

<div id="minor-version-upgrades-should-be-adopted-often">
  ### 应尽量及时进行小版本升级
</div>

强烈建议在新的小版本发布后尽快升级。小版本发行版不会引入破坏性变更，但会包含重要的缺陷修复 (也可能包含安全修复) 。

<div id="test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version">
  ### 在运行目标版本的独立 ClickHouse server 上测试实验性功能
</div>

实验性功能的兼容性随时都可能以任何方式被破坏。如果你正在使用实验性功能，请查看更新日志，并考虑搭建一个安装了目标版本的独立 ClickHouse server，在那里测试这些实验性功能的使用情况。

<div id="downgrades">
  ### 降级
</div>

如果你在升级后发现新版本与所依赖的某些功能不兼容，并且还没有开始使用任何新功能，则或许可以降级到较新的旧版本 (发布时间不超过一年) 。一旦使用了这些新功能，就无法再降级。

<div id="multiple-clickhouse-server-versions-in-a-cluster">
  ### 一个集群中的多个 ClickHouse server 版本
</div>

我们会尽力维持一年的兼容窗口 (其中包括 2 个长期支持版) 。这意味着，如果两个版本之间相差不到一年 (或者它们之间相隔的长期支持版少于两个) ，那么任意两个版本理论上都应该能够在同一个集群中协同工作。不过，仍建议尽快将集群中的所有成员升级到相同版本，因为仍有可能出现一些小问题 (例如分布式查询变慢、ReplicatedMergeTree 中某些后台操作出现可重试错误等) 。

如果不同版本的发布日期相差超过一年，我们绝不建议在同一个集群中混合运行这些版本。虽然我们预计不会发生数据丢失，但集群可能会变得无法使用。如果版本差距超过一年，可能出现的问题包括：

* 集群可能无法工作
* 部分 (甚至全部) 查询可能会因各种错误而失败
* 日志中可能会出现各种错误/警告
* 可能无法降级

<div id="incremental-upgrades">
  ### 增量升级
</div>

如果当前版本与目标版本之间相差超过一年，建议采用以下任一方式：

* 在停机情况下升级 (停止所有服务器，升级所有服务器，然后重新启动所有服务器) 。
* 或先升级到一个中间版本 (该版本比当前版本新，但不超过一年) 。

<div id="recommended-plan">
  ## 推荐方案
</div>

以下是实现 ClickHouse 零停机升级的推荐步骤：

1. 确保你的配置更改不在默认的 `/etc/clickhouse-server/config.xml` 文件中，而是放在 `/etc/clickhouse-server/config.d/` 中，因为 `/etc/clickhouse-server/config.xml` 可能会在升级过程中被覆盖。
2. 通读[更新日志](/zh/whats-new/changelog/index.md)中的破坏性变更 (从目标发布版本一路回溯到你当前使用的发布版本) 。
3. 对破坏性变更中识别出的、可在升级前完成的内容进行更新，并列出需要在升级后完成的变更。
4. 为每个分片确定一个或多个副本，以便在升级该分片其余副本时，这些副本保持在线。
5. 对即将升级的副本逐个执行：

* 关闭 ClickHouse server
* 将 server 升级到目标版本
* 启动 ClickHouse server
* 等待 Keeper 消息表明系统已恢复稳定
* 继续处理下一个副本6. 检查 Keeper 日志和 ClickHouse 日志中是否有错误

7. 将步骤 4 中确定的副本升级到新版本
8. 参考步骤 1 到 3 中整理的变更列表，完成那些需要在升级后进行的更改。

:::note
在复制环境中运行多个版本的 ClickHouse 时，出现此错误消息是预期现象。当所有副本都升级到相同版本后，你将不再看到这些错误消息。

```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```

:::

<div id="clickhouse-server-binary-upgrade-process">
  ## ClickHouse server 可执行文件升级流程
</div>

如果 ClickHouse 是通过 `deb` 软件包安装的，请在服务器上执行以下命令：

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

如果您安装 ClickHouse 时使用的不是推荐的 `deb` 软件包，请采用相应的更新方式。

:::note
只要不出现某个分片的所有副本同时离线的情况，您就可以同时更新多台服务器。
:::

将旧版本的 ClickHouse 升级到特定版本：

例如：

`xx.yy.a.b` 是当前的稳定版本。最新的稳定版本可在[这里](https://github.com/ClickHouse/ClickHouse/releases)查看。

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```