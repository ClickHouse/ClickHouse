# 用于查询处理的外部数据 {#external-data-for-query-processing}

ClickHouse 允许向服务器发送处理查询所需的数据以及 SELECT 查询。这些数据放在一个临时表中（请参阅 «临时表» 一节），可以在查询中使用（例如，在 IN 操作符中）。

例如，如果您有一个包含重要用户标识符的文本文件，则可以将其与使用此列表过滤的查询一起上传到服务器。

如果需要使用大量外部数据运行多个查询，请不要使用该特性。最好提前把数据上传到数据库。

可以使用命令行客户端（在非交互模式下）或使用 HTTP 接口上传外部数据。

在命令行客户端中，您可以指定格式的参数部分

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

对于传输的表的数量，可能有多个这样的部分。

**–external** – 标记子句的开始。
**–file** – 带有表存储的文件的路径，或者，它指的是STDIN。
只能从 stdin 中检索单个表。

以下的参数是可选的：**–name** – 表的名称，如果省略，则采用 \_data。
**–format** – 文件中的数据格式。 如果省略，则使用 TabSeparated。

以下的参数必选一个：**–types** – 逗号分隔列类型的列表。例如：`UInt64,String`。列将被命名为 \_1，\_2，…
**–structure**– 表结构的格式 `UserID UInt64`，`URL String`。定义列的名字以及类型。

在 «file» 中指定的文件将由 «format» 中指定的格式解析，使用在 «types» 或 «structure» 中指定的数据类型。该表将被上传到服务器，并在作为名称为 «name»临时表。

示例：

``` bash
echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

当使用HTTP接口时，外部数据以 multipart/form-data 格式传递。每个表作为一个单独的文件传输。表名取自文件名。«query\_string» 传递参数 «name\_format»、«name\_types»和«name\_structure»，其中 «name» 是这些参数对应的表的名称。参数的含义与使用命令行客户端时的含义相同。

示例：

``` bash
cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

对于分布式查询，将临时表发送到所有远程服务器。

[原始文章](https://clickhouse.tech/docs/zh/operations/table_engines/external_data/) <!--hide-->
