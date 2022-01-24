---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u6982\u8FF0"
---

# 外部字典 {#dicts-external-dicts}

您可以从各种数据源添加自己的字典。 字典的数据源可以是本地文本或可执行文件、HTTP(s)资源或其他DBMS。 有关详细信息，请参阅 “[外部字典的来源](external-dicts-dict-sources.md)”.

ClickHouse:

-   完全或部分存储在RAM中的字典。
-   定期更新字典并动态加载缺失的值。 换句话说，字典可以动态加载。
-   允许创建外部字典与xml文件或 [DDL查询](../../statements/create.md#create-dictionary-query).

外部字典的配置可以位于一个或多个xml文件中。 配置的路径在指定 [dictionaries_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config) 参数。

字典可以在服务器启动或首次使用时加载，具体取决于 [dictionaries_lazy_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) 设置。

该 [字典](../../../operations/system-tables/dictionaries.md#system_tables-dictionaries) 系统表包含有关在服务器上配置的字典的信息。 对于每个字典，你可以在那里找到:

-   字典的状态。
-   配置参数。
-   度量指标，如为字典分配的RAM量或自成功加载字典以来的查询数量。

字典配置文件具有以下格式:

``` xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</clickhouse>
```

你可以 [配置](external-dicts-dict.md) 同一文件中的任意数量的字典。

[字典的DDL查询](../../statements/create.md#create-dictionary-query) 在服务器配置中不需要任何其他记录。 它们允许使用字典作为一流的实体，如表或视图。

!!! attention "注意"
    您可以通过在一个小字典中描述它来转换小字典的值 `SELECT` 查询（见 [变换](../../../sql-reference/functions/other-functions.md) 功能）。 此功能与外部字典无关。

## 另请参阅 {#ext-dicts-see-also}

-   [配置外部字典](external-dicts-dict.md)
-   [在内存中存储字典](external-dicts-dict-layout.md)
-   [字典更新](external-dicts-dict-lifetime.md)
-   [外部字典的来源](external-dicts-dict-sources.md)
-   [字典键和字段](external-dicts-dict-structure.md)
-   [使用外部字典的函数](../../../sql-reference/functions/ext-dict-functions.md)

[原始文章](https://clickhouse.com/docs/en/query_language/dicts/external_dicts/) <!--hide-->
