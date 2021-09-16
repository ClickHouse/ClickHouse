---
toc_priority: 1
toc_title: 云
---

# ClickHouse云服务提供商 {#clickhouse-cloud-service-providers}

!!! info "注意"
    如果您已经推出具有托管ClickHouse服务的公共云，请随时[提交一个 pull request](https://github.com/ClickHouse/ClickHouse/edit/master/docs/en/commercial/cloud.md)将其添加到以下列表。

## Yandex云 {#yandex-cloud}

[Yandex ClickHouse托管服务](https://cloud.yandex.com/services/managed-clickhouse?utm_source=referrals&utm_medium=clickhouseofficialsite&utm_campaign=link3)提供以下主要功能:

-   用于[ClickHouse replication](../engines/table-engines/mergetree-family/replication.md)的完全托管的ZooKeeper服务
-   多种存储类型选择
-   不同可用区副本
-   加密与隔离
-   自动化维护

## Altinity.Cloud {#altinity.cloud}

[Altinity.Cloud](https://altinity.com/cloud-database/)是针对Amazon公共云的完全托管的ClickHouse-as-a-Service

-   在Amazon资源上快速部署ClickHouse集群
-   轻松进行横向扩展/纵向扩展以及节点的垂直扩展
-   具有公共端点或VPC对等的租户隔离
-   可配置存储类型以及卷配置
-   跨可用区扩展以实现性能和高可用性
-   内置监控和SQL查询编辑器

## 阿里云{#alibaba-cloud}

[阿里云ClickHouse托管服务](https://www.alibabacloud.com/zh/product/clickhouse)提供以下主要功能：

- 基于阿里飞天分布式系统的高可靠云盘存储引擎
- 按需扩容，无需手动进行数据搬迁
- 支持单节点、单副本、多节点、多副本多种架构，支持冷热数据分层
- 支持访问白名单和一键恢复，多层网络安全防护，云盘加密
- 与云上日志系统、数据库、数据应用工具无缝集成
- 内置监控和数据库管理平台
- 专业的数据库专家技术支持和服务

## SberCloud {#sbercloud}

[SberCloud.Advanced](https://sbercloud.ru/en/advanced)提供[MapReduce Service (MRS)](https://docs.sbercloud.ru/mrs/ug/topics/ug__clickhouse.html), 一个可靠、安全且易于使用的企业级平台，用于存储、处理和分析大数据。MRS允许您快速创建和管理ClickHouse集群。

-   一个ClickHouse实例由三个ZooKeeper节点和多个ClickHouse节点组成。 Dedicated Replica模式用于保证双数据副本的高可靠性。
-   MRS提供平滑弹性伸缩能力，快速满足集群存储容量或CPU计算资源不足场景下的业务增长需求。当您扩展集群中ClickHouse节点的容量时，MRS提供一键式数据平衡工具，让您主动进行数据平衡。 您可以根据业务特点确定数据均衡方式和时间，保证业务的可用性，实现平滑扩展。
-   MRS采用弹性负载均衡保障高可用部署架构，自动将用户访问流量分配到多个后端节点，将服务能力扩展到外部系统，提高容错能力。 通过ELB轮询机制，数据写入本地表，从不同节点的分布式表中读取。 这样就保证了数据读写负载和应用访问的高可用。

## 腾讯云{#tencent-cloud}

[腾讯云ClickHouse托管服务](https://cloud.tencent.com/product/cdwch)提供以下主要功能：

-   易于在腾讯云上部署和管理
-   高度可扩展和可用
-   集成监控和警报服务
-   每个集群VPC隔离的高安全性
-   按需定价，无前期成本或长期承诺

{## [原始文章](https://clickhouse.tech/docs/en/commercial/cloud/) ##}
