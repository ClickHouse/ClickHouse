---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "\u5B57\u5178\u66F4\u65B0"
---

# 字典更新 {#dictionary-updates}

ClickHouse定期更新字典。 完全下载字典的更新间隔和缓存字典的无效间隔在 `<lifetime>` 在几秒钟内标记。

字典更新（除首次使用的加载之外）不会阻止查询。 在更新期间，将使用旧版本的字典。 如果在更新过程中发生错误，则将错误写入服务器日志，并使用旧版本的字典继续查询。

设置示例:

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

设置 `<lifetime>0</lifetime>` (`LIFETIME(0)`）防止字典更新。

您可以设置升级的时间间隔，ClickHouse将在此范围内选择一个统一的随机时间。 为了在大量服务器上升级时分配字典源上的负载，这是必要的。

设置示例:

``` xml
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

``` sql
LIFETIME(MIN 300 MAX 360)
```

如果 `<min>0</min>` 和 `<max>0</max>`，ClickHouse不会按超时重新加载字典。
在这种情况下，如果字典配置文件已更改，ClickHouse可以更早地重新加载字典 `SYSTEM RELOAD DICTIONARY` 命令被执行。

升级字典时，ClickHouse服务器根据字典的类型应用不同的逻辑 [来源](external-dicts-dict-sources.md):

升级字典时，ClickHouse服务器根据字典的类型应用不同的逻辑 [来源](external-dicts-dict-sources.md):

-   对于文本文件，它检查修改的时间。 如果时间与先前记录的时间不同，则更新字典。
-   对于MyISAM表，修改的时间使用检查 `SHOW TABLE STATUS` 查询。
-   默认情况下，每次都会更新来自其他来源的字典。

对于MySQL（InnoDB），ODBC和ClickHouse源代码，您可以设置一个查询，只有在字典真正改变时才会更新字典，而不是每次都更新。 为此，请按照下列步骤操作:

-   字典表必须具有在源数据更新时始终更改的字段。
-   源的设置必须指定检索更改字段的查询。 ClickHouse服务器将查询结果解释为一行，如果此行相对于其以前的状态发生了更改，则更新字典。 指定查询 `<invalidate_query>` 字段中的设置 [来源](external-dicts-dict-sources.md).

设置示例:

``` xml
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

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

[原始文章](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_lifetime/) <!--hide-->
