---
slug: /en/sql-reference/dictionaries/external-dictionaries/external-dicts
sidebar_position: 39
sidebar_label: General Description
---
import CloudDetails from '@site/docs/en/sql-reference/dictionaries/external-dictionaries/_snippet_dictionary_in_cloud.md';

# Dictionaries 

:::tip Tutorial
If you are getting started with Dictionaries in ClickHouse we have a tutorial that covers that topic.  Take a look [here](/docs/en/tutorial.md).
:::

You can add your own dictionaries from various data sources. The source for a dictionary can be a ClickHouse table, a local text or executable file, an HTTP(s) resource, or another DBMS. For more information, see “[Dictionary Sources](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md)”.

ClickHouse:

-   Fully or partially stores dictionaries in RAM.
-   Periodically updates dictionaries and dynamically loads missing values. In other words, dictionaries can be loaded dynamically.
-   Allows creating dictionaries with xml files or [DDL queries](../../../sql-reference/statements/create/dictionary.md).

The configuration of dictionaries can be located in one or more xml-files. The path to the configuration is specified in the [dictionaries_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config) parameter.

Dictionaries can be loaded at server startup or at first use, depending on the [dictionaries_lazy_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) setting.

The [dictionaries](../../../operations/system-tables/dictionaries.md#system_tables-dictionaries) system table contains information about dictionaries configured at server. For each dictionary you can find there:

-   Status of the dictionary.
-   Configuration parameters.
-   Metrics like amount of RAM allocated for the dictionary or a number of queries since the dictionary was successfully loaded.

<CloudDetails />

## Creating a dictionary with a DDL query

Dictionaries can be created with [DDL queries](../../../sql-reference/statements/create/dictionary.md), and this is the recommended method because with DDL created dictionaries:
- No additional records are added to server configuration files
- The dictionaries can be worked with as first-class entities, like tables or views
- Data can be read directly, using familiar SELECT rather than dictionary table functions
- The dictionaries can be easily renamed

## Creating a dictionary with a configuration file

:::note
Creating a dictionary with a configuration file is not applicable to ClickHouse Cloud. Please use DDL (see above), and create your dictionary as user `default`.
:::

The dictionary configuration file has the following format:

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

You can [configure](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md) any number of dictionaries in the same file.


:::note    
You can convert values for a small dictionary by describing it in a `SELECT` query (see the [transform](../../../sql-reference/functions/other-functions.md) function). This functionality is not related to dictionaries.
:::

## See Also

-   [Configuring a Dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md)
-   [Storing Dictionaries in Memory](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md)
-   [Dictionary Updates](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md)
-   [Dictionary Sources](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md)
-   [Dictionary Key and Fields](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md)
-   [Functions for Working with Dictionaries](../../../sql-reference/functions/ext-dict-functions.md)

## Related Content

- [Using dictionaries to accelerate queries](https://clickhouse.com/blog/faster-queries-dictionaries-clickhouse)
