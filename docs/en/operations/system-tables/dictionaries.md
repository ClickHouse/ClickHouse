# system.dictionaries {#system_tables-dictionaries}

Contains information about [external dictionaries](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([String](../../sql-reference/data-types/string.md)) — [Dictionary name](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../../sql-reference/statements/system.md#query_language-system-reload-dictionary) query, timeout, dictionary config has changed).
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([String](../../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([String](../../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [Storing Dictionaries in Memory](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Key type](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Numeric Key ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([String](../../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Array of [attribute names](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) provided by the dictionary.
-   `attribute.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Corresponding array of [attribute types](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) that are provided by the dictionary.
-   `bytes_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([String](../../sql-reference/data-types/string.md)) — Text describing the [data source](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) for the dictionary.
-   `lifetime_min` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [lifetime](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if `invalidate_query` is set, then only if it has changed). Set in seconds.
-   `lifetime_max` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [lifetime](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if `invalidate_query` is set, then only if it has changed). Set in seconds.
-   `loading_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn’t be created.

**Example**

Configure the dictionary.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Make sure that the dictionary is loaded.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```
