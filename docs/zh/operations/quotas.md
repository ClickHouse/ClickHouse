# 配额 {#quotas}

配额允许您在一段时间内限制资源使用情况，或者只是跟踪资源的使用。
配额在用户配置中设置。 这通常是 ‘users.xml’.

The system also has a feature for limiting the complexity of a single query. See the section «Restrictions on query complexity»).

与查询复杂性限制相比，配额:

-   对可以在一段时间内运行的一组查询设置限制，而不是限制单个查询。
-   占用在所有远程服务器上用于分布式查询处理的资源。

让我们来看看的部分 ‘users.xml’ 定义配额的文件。

``` xml
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
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

默认情况下，配额只跟踪每小时的资源消耗，而不限制使用情况。
每次请求后，计算出的每个时间间隔的资源消耗将输出到服务器日志中。

``` xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

为 ‘statbox’ 配额，限制设置为每小时和每24小时（86,400秒）。 时间间隔从实现定义的固定时刻开始计数。 换句话说，24小时间隔不一定从午夜开始。

间隔结束时，将清除所有收集的值。 在下一个小时内，配额计算将重新开始。

以下是可以限制的金额:

`queries` – The total number of requests.

`errors` – The number of queries that threw an exception.

`result_rows` – The total number of rows given as the result.

`read_rows` – The total number of source rows read from tables for running the query, on all remote servers.

`execution_time` – The total query execution time, in seconds (wall time).

如果在至少一个时间间隔内超出限制，则会引发异常，其中包含有关超出了哪个限制、哪个时间间隔以及新时间间隔开始时（何时可以再次发送查询）的文本。

Quotas can use the «quota key» feature in order to report on resources for multiple keys independently. Here is an example of this:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip /> so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

配额分配给用户 ‘users’ section of the config. See the section «Access rights».

For distributed query processing, the accumulated amounts are stored on the requestor server. So if the user goes to another server, the quota there will «start over».

服务器重新启动时，将重置配额。

[原始文章](https://clickhouse.tech/docs/en/operations/quotas/) <!--hide-->
