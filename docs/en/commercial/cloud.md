---
toc_priority: 1
toc_title: Cloud
---

# ClickHouse Cloud Service Providers {#clickhouse-cloud-service-providers}

!!! info "Info"
    If you have launched a public cloud with managed ClickHouse service, feel free to [open a pull-request](https://github.com/ClickHouse/ClickHouse/edit/master/docs/en/commercial/cloud.md) adding it to the following list.

## Yandex Cloud {#yandex-cloud}

[Yandex Managed Service for ClickHouse](https://cloud.yandex.com/services/managed-clickhouse?utm_source=referrals&utm_medium=clickhouseofficialsite&utm_campaign=link3) provides the following key features:

-   Fully managed ZooKeeper service for [ClickHouse replication](../engines/table-engines/mergetree-family/replication.md)
-   Multiple storage type choices
-   Replicas in different availability zones
-   Encryption and isolation
-   Automated maintenance

## Altinity.Cloud {#altinity.cloud}

[Altinity.Cloud](https://altinity.com/cloud-database/) is a fully managed ClickHouse-as-a-Service for the Amazon public cloud.

-   Fast deployment of ClickHouse clusters on Amazon resources
-   Easy scale-out/scale-in as well as vertical scaling of nodes
-   Isolated per-tenant VPCs with public endpoint or VPC peering
-   Configurable storage types and volume configurations
-   Cross-AZ scaling for performance and high availability
-   Built-in monitoring and SQL query editor

## Alibaba Cloud {#alibaba-cloud}

[Alibaba Cloud Managed Service for ClickHouse](https://www.alibabacloud.com/product/clickhouse) provides the following key features:

-   Highly reliable cloud disk storage engine based on [Alibaba Cloud Apsara](https://www.alibabacloud.com/product/apsara-stack) distributed system
-   Expand capacity on demand without manual data migration
-   Support single-node, single-replica, multi-node, and multi-replica architectures, and support hot and cold data tiering
-   Support access allow-list, one-key recovery, multi-layer network security protection, cloud disk encryption
-   Seamless integration with cloud log systems, databases, and data application tools
-   Built-in monitoring and database management platform
-   Professional database expert technical support and service

## SberCloud {#sbercloud}

[SberCloud.Advanced](https://sbercloud.ru/en/advanced) provides [MapReduce Service (MRS)](https://docs.sbercloud.ru/mrs/ug/topics/ug__clickhouse.html), a reliable, secure, and easy-to-use enterprise-level platform for storing, processing, and analyzing big data. MRS allows you to quickly create and manage ClickHouse clusters.

-   A ClickHouse instance consists of three ZooKeeper nodes and multiple ClickHouse nodes. The Dedicated Replica mode is used to ensure high reliability of dual data copies.
-   MRS provides smooth and elastic scaling capabilities to quickly meet service growth requirements in scenarios where the cluster storage capacity or CPU computing resources are not enough. When you expand the capacity of ClickHouse nodes in a cluster, MRS provides a one-click data balancing tool and gives you the initiative to balance data. You can determine the data balancing mode and time based on service characteristics to ensure service availability, implementing smooth scaling.
-   MRS uses the Elastic Load Balance ensuring high availability deployment architecture to automatically distribute user access traffic to multiple backend nodes, expanding service capabilities to external systems and improving fault tolerance. With the ELB polling mechanism, data is written to local tables and read from distributed tables on different nodes. In this way, data read/write load and high availability of application access are guaranteed.

## Tencent Cloud {#tencent-cloud}

[Tencent Managed Service for ClickHouse](https://cloud.tencent.com/product/cdwch) provides the following key features:

-   Easy to deploy and manage on Tencent Cloud
-   Highly scalable and available
-   Integrated monitor and alert service
-   High security with isolated per cluster VPCs
-   On-demand pricing with no upfront costs or long-term commitments

{## [Original article](https://clickhouse.tech/docs/en/commercial/cloud/) ##}
