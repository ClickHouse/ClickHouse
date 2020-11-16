---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。字典 {#system_tables-dictionaries}

包含以下信息 [外部字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

列:

-   `database` ([字符串](../../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([字符串](../../sql-reference/data-types/string.md)) — [字典名称](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([枚举8](../../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../../sql-reference/statements/system.md#query_language-system-reload-dictionary) 查询，超时，字典配置已更改）。
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([字符串](../../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([字符串](../../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [在内存中存储字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [密钥类型](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key):数字键 ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([字符串](../../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([阵列](../../sql-reference/data-types/array.md)([字符串](../../sql-reference/data-types/string.md))) — Array of [属性名称](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 由字典提供。
-   `attribute.types` ([阵列](../../sql-reference/data-types/array.md)([字符串](../../sql-reference/data-types/string.md))) — Corresponding array of [属性类型](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 这是由字典提供。
-   `bytes_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([字符串](../../sql-reference/data-types/string.md)) — Text describing the [数据源](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) 为了字典
-   `lifetime_min` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [使用寿命](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) 在内存中的字典，之后ClickHouse尝试重新加载字典（如果 `invalidate_query` 被设置，那么只有当它已经改变）。 在几秒钟内设置。
-   `lifetime_max` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [使用寿命](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) 在内存中的字典，之后ClickHouse尝试重新加载字典（如果 `invalidate_query` 被设置，那么只有当它已经改变）。 在几秒钟内设置。
-   `loading_start_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([字符串](../../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**示例**

配置字典。

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

确保字典已加载。

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```
