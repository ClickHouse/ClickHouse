---
toc_priority: 44
toc_title: Requirements
---

# Requirements {#requirements}

## CPU {#cpu}

For installation from prebuilt deb packages, use a CPU with x86\_64 architecture and support for SSE 4.2 instructions. To run ClickHouse with processors that do not support SSE 4.2 or have AArch64 or PowerPC64LE architecture, you should build ClickHouse from sources.

ClickHouse implements parallel data processing and uses all the hardware resources available. When choosing a processor, take into account that ClickHouse works more efficiently at configurations with a large number of cores but a lower clock rate than at configurations with fewer cores and a higher clock rate. For example, 16 cores with 2600 MHz is preferable to 8 cores with 3600 MHz.

It is recommended to use **Turbo Boost** and **hyper-threading** technologies. It significantly improves performance with a typical workload.

## RAM {#ram}

We recommend using a minimum of 4GB of RAM to perform non-trivial queries. The ClickHouse server can run with a much smaller amount of RAM, but it requires memory for processing queries.

The required volume of RAM depends on:

-   The complexity of queries.
-   The amount of data that is processed in queries.

To calculate the required volume of RAM, you should estimate the size of temporary data for [GROUP BY](../sql-reference/statements/select/group-by.md#select-group-by-clause), [DISTINCT](../sql-reference/statements/select/distinct.md#select-distinct), [JOIN](../sql-reference/statements/select/join.md#select-join) and other operations you use.

ClickHouse can use external memory for temporary data. See [GROUP BY in External Memory](../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory) for details.

## Swap File {#swap-file}

Disable the swap file for production environments.

## Storage Subsystem {#storage-subsystem}

You need to have 2GB of free disk space to install ClickHouse.

The volume of storage required for your data should be calculated separately. Assessment should include:

-   Estimation of the data volume.

    You can take a sample of the data and get the average size of a row from it. Then multiply the value by the number of rows you plan to store.

-   The data compression coefficient.

    To estimate the data compression coefficient, load a sample of your data into ClickHouse, and compare the actual size of the data with the size of the table stored. For example, clickstream data is usually compressed by 6-10 times.

To calculate the final volume of data to be stored, apply the compression coefficient to the estimated data volume. If you plan to store data in several replicas, then multiply the estimated volume by the number of replicas.

## Network {#network}

If possible, use networks of 10G or higher class.

The network bandwidth is critical for processing distributed queries with a large amount of intermediate data. Besides, network speed affects replication processes.

## Software {#software}

ClickHouse is developed primarily for the Linux family of operating systems. The recommended Linux distribution is Ubuntu. The `tzdata` package should be installed in the system.

ClickHouse can also work in other operating system families. See details in the [Getting started](../getting-started/index.md) section of the documentation.
