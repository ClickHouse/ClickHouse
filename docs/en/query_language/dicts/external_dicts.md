# External Dictionaries {#dicts-external_dicts}

You can add your own dictionaries from various data sources. The data source for a dictionary can be a local text or executable file, an HTTP(s) resource, or another DBMS. For more information, see "[Sources for external dictionaries](external_dicts_dict_sources.md)".

ClickHouse:

> - Fully or partially stores dictionaries in RAM.
- Periodically updates dictionaries and dynamically loads missing values. In other words, dictionaries can be loaded dynamically.

The configuration of external dictionaries is located in one or more files. The path to the configuration is specified in the [dictionaries_config](../../operations/server_settings/settings.md#server_settings-dictionaries_config) parameter.

Dictionaries can be loaded at server startup or at first use, depending on the [dictionaries_lazy_load](../../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load) setting.

The dictionary configuration file has the following format:

```xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</yandex>
```

You can [configure](external_dicts_dict.md) any number of dictionaries in the same file.

!!! attention
    You can convert values for a small dictionary by describing it in a `SELECT` query (see the [transform](../functions/other_functions.md) function). This functionality is not related to external dictionaries.

**See also**

- [Functions for Working with External Dictionaries](../functions/ext_dict_functions.md)


[Original article](https://clickhouse.yandex/docs/en/query_language/dicts/external_dicts/) <!--hide-->
