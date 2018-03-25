<a name="dicts-external_dicts"></a>

# External dictionaries

You can add your own dictionaries from various data sources. The data source for a dictionary can be a local text or executable file, an HTTP(s) resource, or another DBMS. For more information, see "[Sources for external dictionaries](external_dicts_dict_sources.md#dicts-external_dicts_dict_sources)".

ClickHouse:

> - Fully or partially stores dictionaries in RAM.
- Periodically updates dictionaries and dynamically loads missing values. In other words, dictionaries can be loaded dynamically.

The configuration of external dictionaries is located in one or more files. The path to the configuration is specified in the [dictionaries_config](../operations/server_settings/settings.md#server_settings-dictionaries_config) parameter.

Dictionaries can be loaded at server startup or at first use, depending on the [dictionaries_lazy_load](../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load) setting.

The dictionary config file has the following format:

```xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>
    
    <dictionary>
        <!-- Dictionary configuration -->    
    </dictionary>
            
    ...

    <dictionary>
        <!-- Dictionary configuration -->
    </dictionary>
</yandex>
```

You can [configure](external_dicts_dict.md#dicts-external_dicts_dict) any number of dictionaries in the same file. The file format is preserved even if there is only one dictionary (i.e. `<yandex><dictionary> <!--configuration -> </dictionary></yandex>` ).

See also "[Functions for working with external dictionaries](../functions/ext_dict_functions.md#ext_dict_functions)".

<div class="admonition attention">

You can convert values ​​for a small dictionary by describing it in a `SELECT` query (see the [transform](../functions/other_functions.md#other_functions-transform) function). This functionality is not related to external dictionaries.

</div>

```eval_rst
.. toctree::
    :glob:
   
    external_dicts_dict*
```

