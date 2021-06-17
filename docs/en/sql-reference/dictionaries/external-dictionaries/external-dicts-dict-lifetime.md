---
toc_priority: 42
toc_title: Dictionary Updates
---

# Dictionary Updates {#dictionary-updates}

ClickHouse periodically updates the dictionaries. The update interval for fully downloaded dictionaries and the invalidation interval for cached dictionaries are defined in the `<lifetime>` tag in seconds.

Dictionary updates (other than loading for first use) do not block queries. During updates, the old version of a dictionary is used. If an error occurs during an update, the error is written to the server log, and queries continue using the old version of dictionaries.

Example of settings:

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

or

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

Setting `<lifetime>0</lifetime>` (`LIFETIME(0)`) prevents dictionaries from updating.

You can set a time interval for updates, and ClickHouse will choose a uniformly random time within this range. This is necessary in order to distribute the load on the dictionary source when updating on a large number of servers.

Example of settings:

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

or

``` sql
LIFETIME(MIN 300 MAX 360)
```

If `<min>0</min>` and `<max>0</max>`, ClickHouse does not reload the dictionary by timeout.
In this case, ClickHouse can reload the dictionary earlier if the dictionary configuration file was changed or the `SYSTEM RELOAD DICTIONARY` command was executed.

When updating the dictionaries, the ClickHouse server applies different logic depending on the type of [source](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md):

-   For a text file, it checks the time of modification. If the time differs from the previously recorded time, the dictionary is updated.
-   For MySQL source, the time of modification is checked using a `SHOW TABLE STATUS` query (in case of MySQL 8 you need to disable meta-information caching in MySQL by `set global information_schema_stats_expiry=0`.
-   Dictionaries from other sources are updated every time by default.

For other sources (ODBC, PostgreSQL, ClickHouse, etc), you can set up a query that will update the dictionaries only if they really changed, rather than each time. To do this, follow these steps:

-   The dictionary table must have a field that always changes when the source data is updated.
-   The settings of the source must specify a query that retrieves the changing field. The ClickHouse server interprets the query result as a row, and if this row has changed relative to its previous state, the dictionary is updated. Specify the query in the `<invalidate_query>` field in the settings for the [source](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md).

Example of settings:

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

or

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

For `Cache`, `ComplexKeyCache`, `SSDCache`, and `SSDComplexKeyCache` dictionaries both synchronious and asynchronious updates are supported.

It is also possible for `Flat`, `Hashed`, `ComplexKeyHashed` dictionaries to only request data that was changed after previous update. If `update_field` is specified in as part of dictionary source configuration value of previous update time in seconds will be added to data request. Depends of source type Executable, HTTP, MySQL, PostgreSQL, ClickHouse, ODBC different logic will be applied to `update_field` before request data from external source.

-   If source is HTTP then `update_field` will be added as query parameter with last update time as parameter value.
-   If source is Executable then `update_field` will be added as executable script argument with last update time as argument value.
-   If source is ClickHouse, MySQL, PostgreSQL, ODBC there will be additional part of WHERE, where `update_field` is compared as greater or equal with last update time.

If `update_field` option is set. Additional option `update_lag` can be set. Value of `update_lag` option is subtracted from previous update time before request updated data.

Example of settings:

``` xml
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

or

``` sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```