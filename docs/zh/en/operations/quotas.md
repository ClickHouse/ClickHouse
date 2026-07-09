---
description: '在 ClickHouse 中配置和管理资源使用配额的指南'
sidebar_label: '配额'
sidebar_position: 51
slug: /operations/quotas
title: '配额'
doc_type: 'guide'
---

:::note ClickHouse Cloud 中的配额
ClickHouse Cloud 支持配额，但必须使用 [DDL 语法](/zh/sql-reference/statements/create/quota) 创建。下文介绍的 XML 配置方式**不受支持**。
:::

配额可用于在一段时间内限制资源使用，或跟踪资源使用情况。
配额在用户配置中设置，通常是 &#39;users.xml&#39;。

系统还提供了限制单个查询复杂度的功能。请参阅 [查询复杂度限制](../operations/settings/query-complexity.md) 一节。

与查询复杂度限制不同，配额：

* 配额针对一段时间内可运行的一组查询进行限制，而不是限制单个查询。
* 在分布式查询处理中，会将所有远程服务器上消耗的资源一并计入。

下面来看一下 &#39;users.xml&#39; 文件中定义配额的这一部分。

```xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <query_selects>0</query_selects>
            <query_inserts>0</query_inserts>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

默认情况下，配额会按小时统计资源消耗，但不限制使用量。
每个时间间隔计算出的资源消耗都会在每次请求后输出到服务器日志中。

```xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <query_selects>100</query_selects>
        <query_inserts>100</query_inserts>
        <written_bytes>5000000</written_bytes>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
        <failed_sequential_authentications>5</failed_sequential_authentications>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <query_selects>10000</query_selects>
        <query_inserts>10000</query_inserts>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <result_bytes>160000000000</result_bytes>
        <read_rows>500000000000</read_rows>
        <result_bytes>16000000000000</result_bytes>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

对于 &#39;statbox&#39; 配额，会按每小时和每 24 小时 (86,400 秒) 设置限制。时间间隔从某个由具体实现决定的固定时刻开始计算。换句话说，24 小时的时间间隔不一定从午夜开始。

当时间间隔结束时，所有已收集的值都会清空。接下来的一个小时内，配额计算将重新开始。

以下是可受限制的数量：

`queries` – 请求总数。

`query_selects` – select 请求总数。

`query_inserts` – insert 请求总数。

`errors` – 抛出异常的查询数量。

`result_rows` – 结果中返回的行总数。

`result_bytes` - 结果中返回的行总大小。

`read_rows` – 在所有远程 server 上运行查询时，从表中读取的源行总数。

`read_bytes` - 在所有远程 server 上运行查询时，从表中读取的总字节数。

`written_bytes` - 写入操作的总字节数。

`execution_time` – 查询执行时间总计，以秒为单位 (墙钟时间) 。

`failed_sequential_authentications` - 连续身份验证错误总数。

`queries_per_normalized_hash` – 任一单个归一化查询的最大执行次数。归一化查询是指将字面量替换为占位符后的查询，因此 `SELECT 1` 和 `SELECT 2` 会被视为同一个归一化查询。此限制会针对每种不同的归一化查询模式分别独立跟踪。

如果在至少一个时间间隔内超出该限制，则会抛出异常，异常文本会说明超出的是哪项限制、对应哪个时间间隔，以及新的时间间隔何时开始 (届时可以再次发送查询) 。

配额可以使用“配额键”功能，对多个键的资源使用情况分别独立进行统计。示例如下：

```xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)

        Instead of <keyed_by_ip /> you can use <keyed_by_forwarded_ip />, so the address
        from the X-Forwarded-For header is used as the quota key.

        For both <keyed_by_ip /> and <keyed_by_forwarded_ip /> you can additionally specify
        <ipv4_prefix_bits> and <ipv6_prefix_bits> to group clients by subnet instead of by a
        single address: the IP address is masked to the given prefix length before being used
        as the quota key. For example, <ipv4_prefix_bits>24</ipv4_prefix_bits> shares one bucket
        across a /24 IPv4 subnet, and <ipv6_prefix_bits>64</ipv6_prefix_bits> across a /64 IPv6
        subnet. These elements can only be used together with <keyed_by_ip /> or
        <keyed_by_forwarded_ip />.
    -->
    <keyed />
```

你也可以使用归一化查询哈希作为 QUOTA 的键，这样每种不同的查询模式都会获得各自独立的 QUOTA bucket。在 XML 配置中，写法如下 `<keyed_by_normalized_query_hash />`：

```xml
<my_quota>
    <keyed_by_normalized_query_hash />
    <interval>
        <duration>3600</duration>
        <queries>100</queries>
    </interval>
</my_quota>
```

同样的内容也可以用 DDL 语法表示：

```sql
CREATE QUOTA my_quota KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO my_user;
```

在此示例中，用户每小时对每种不同的归一化查询最多可执行 100 次。`SELECT number FROM numbers(1)` 和 `SELECT number FROM numbers(2)` 共享同一个 bucket (因为它们具有相同的规范化结果) ，但 `SELECT number, number FROM numbers(1)` 使用单独的 bucket。

配额会在 config 的 &#39;users&#39; 部分分配给用户。请参见 &quot;访问权限&quot; 一节。

对于分布式查询处理，累计计数存储在请求发起方 server 上。因此，如果用户切换到另一台 server，那里的配额将会&quot;重新开始&quot;。

当 server 重启时，配额会被重置。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[使用 ClickHouse 构建单页应用](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)