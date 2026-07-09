---
description: '用于字典自动刷新的 LIFETIME 配置'
sidebar_label: 'LIFETIME'
sidebar_position: 5
slug: /sql-reference/statements/create/dictionary/lifetime
title: '使用 LIFETIME 刷新字典数据'
doc_type: '参考'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

ClickHouse 会根据 `LIFETIME` 标签 (以秒为单位定义) 定期更新字典。
`LIFETIME` 是完整下载型字典的更新间隔，也是缓存型字典的失效间隔。

在更新期间，仍可查询字典的旧版本。
字典更新不会阻塞查询，首次加载时除外。
如果更新期间发生错误，错误会写入服务器日志，查询也可以继续使用字典的旧版本。
如果字典更新成功，字典的旧版本会以[原子方式](/zh/concepts/glossary#atomicity)被替换。

设置示例：

<CloudDetails />

```xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

或

```sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

将 `<lifetime>0</lifetime>` (`LIFETIME(0)`) 设为 0 会阻止字典更新。

您可以为更新设置一个时间间隔，ClickHouse 会在该范围内随机选择一个时间点，且分布均匀。这样做是为了在大量服务器进行更新时，将字典源上的负载分散开来。

设置示例：

```xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

或

```sql
LIFETIME(MIN 300 MAX 360)
```

如果 `<min>0</min>` 且 `<max>0</max>`，ClickHouse 不会因超时而重新加载字典。
在这种情况下，如果字典配置文件已更改，或执行了 `SYSTEM RELOAD DICTIONARY` 命令，ClickHouse 也可以提前重新加载字典。

更新字典时，ClickHouse 服务器会根据 [源](./sources/) 的类型采用不同的逻辑：

* 对于文本文件，它会检查修改时间。如果该时间与先前记录的时间不同，则更新字典。
* 默认情况下，来自其他源的字典每次都会更新。

对于其他源 (ODBC、PostgreSQL、ClickHouse 等) ，你可以设置一个查询，使字典仅在确实发生变化时才更新，而不是每次都更新。为此，请按以下步骤操作：

* 字典表必须有一个字段，该字段会在源数据更新时始终变化。
* 源的设置必须指定一个用于获取该变化字段的查询。ClickHouse 服务器会将查询结果解释为一行，如果这一行相对于其先前状态发生了变化，则更新字典。在 [源](./sources/) 的设置中，通过 `<invalidate_query>` 字段指定该查询。

设置示例：

```xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

或

```sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

对于 `Cache`、`ComplexKeyCache`、`SSDCache` 和 `SSDComplexKeyCache` 字典，既支持同步更新，也支持异步更新。

对于 `Flat`、`Hashed`、`HashedArray` 和 `ComplexKeyHashed` 字典，也可以只请求自上次更新以来发生变更的数据。如果在字典源配置中指定了 `update_field`，则会将上次更新时间的秒级值添加到数据请求中。根据源类型 (Executable、HTTP、MySQL、PostgreSQL、ClickHouse 或 ODBC) 的不同，在向外部源请求数据之前，会对 `update_field` 应用不同的处理逻辑。

* 如果源是 HTTP，`update_field` 会作为查询参数添加，参数值为上次更新时间。
* 如果源是 Executable，`update_field` 会作为可执行脚本的参数添加，参数值为上次更新时间。
* 如果源是 ClickHouse、MySQL、PostgreSQL 或 ODBC，则会额外添加一段 `WHERE` 条件，其中将 `update_field` 与上次更新时间进行大于等于比较。
  * 默认情况下，这个 `WHERE` 条件会在 SQL 查询的最外层进行检查。或者，也可以使用 `{condition}` 关键字，在查询中其他任意 `WHERE` 子句内检查该条件。示例：
    ```sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

如果设置了 `update_field` 选项，还可以设置额外的 `update_lag` 选项。请求更新数据之前，会先从上次更新时间中减去 `update_lag` 选项的值。

设置示例：

```xml
<dictionary>
    ...
        <clickhouse>
            ...
            <update_field>added_time</update_field>
            <update_lag>15</update_lag>
        </clickhouse>
    ...
</dictionary>
```

或

```sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```