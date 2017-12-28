<a name="dicts-external_dicts_dict_lifetime"></a>

# Dictionary updates

ClickHouse periodically updates the dictionaries. The update interval for fully downloaded dictionaries and the invalidation interval for cached dictionaries are defined in the `<lifetime>` tag in seconds.

Dictionary updates (other than loading for first use) do not block queries. During updates, the old version of a dictionary is used. If an error occurs during an update, the error is written to the server log, and queries continue using the old version of dictionaries.

Example of settings:

```xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

Setting `  <lifetime> 0</lifetime>  `  prevents updating dictionaries.

You can set a time interval for upgrades, and ClickHouse will choose a uniformly random time within this range. This is necessary in order to distribute the load on the dictionary source when upgrading on a large number of servers.

Example of settings:

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

When upgrading the dictionaries, the ClickHouse server applies different logic depending on the type of [ source](external_dicts_dict_sources.md#dicts-external_dicts_dict_sources):

> - For a text file, it checks the time of modification. If the time differs from the previously recorded time, the dictionary is updated.
> - For MyISAM tables, the time of modification is checked using a `SHOW TABLE STATUS` query.
> - Dictionaries from other sources are updated every time by default.

For MySQL (InnoDB) and ODBC sources, you can set up a query that will update the dictionaries only if they really changed, rather than each time. To do this, follow these steps:

> - The dictionary table must have a field that always changes when the source data is updated.
> - The settings of the source must specify a query that retrieves the changing field. The ClickHouse server interprets the query result as a row, and if this row has changed relative to its previous state, the dictionary is updated. The query must be specified in the `<invalidate_query>` field in the [ source](external_dicts_dict_sources.md#dicts-external_dicts_dict_sources) settings.

Example of settings:

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

