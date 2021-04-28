# system.tables-dictionaries {#system_tables-dictionaries}

包含有关 [外部字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)的信息。

列:

-   `database` ([String](../../sql-reference/data-types/string.md)) — 包含由DDL查询创建的字典的数据库的名称。其他字典的空字符串。
-   `name` ([String](../../sql-reference/data-types/string.md)) — [字典名称](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md)。
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — 字典状态。可能的值：
    -   `NOT_LOADED` — 由于未使用字典，因此未加载字典。
    -   `LOADED` — 字典已成功加载。
    -   `FAILED` — 由于错误而无法加载字典。
    -   `LOADING` — 字典正在加载中。
    -   `LOADED_AND_RELOADING` — 字典已成功加载，并且现在正在重新加载（常见原因：[SYSTEM RELOAD DICTIONARY](../../sql-reference/statements/system.md#query_language-system-reload-dictionary) 查询，超时，字典配置已更改）。
    -   `FAILED_AND_RELOADING` — 由于错误而无法加载字典，现在正在加载。
-   `origin` ([String](../../sql-reference/data-types/string.md)) — 描述字典的配置文件的路径。
-   `type` ([String](../../sql-reference/data-types/string.md)) — 字典分配的类型。 [在内存中存储字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md)。
-   `key` — [密钥类型](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key):数字键 ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) 或复合键 ([String](../../sql-reference/data-types/string.md)) — 形式为 “(类型1，类型2，…，类型n)”。
-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 字典提供的 [属性名称](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 数组。
-   `attribute.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 字典提供的对应 [属性类型](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 数组。
-   `bytes_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 为字典分配的RAM字节数。
-   `query_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 自加载字典或上次成功重新启动之后的查询数。 
-   `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — 对于缓存字典，该值在缓存中使用的百分比。
-   `element_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 字典中存储的元素数。
-   `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — 字典中填充的百分比（对于哈希字典，填充在哈希表中的百分比）。
-   `source` ([String](../../sql-reference/data-types/string.md)) — 描述字典 [数据源](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) 的文本。
-   `lifetime_min` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) —  字典在内存中的最小 [生存期](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md)，之后ClickHouse尝试重新加载字典（如果 `invalidate_query` 已设置，则仅在字典已更改的情况下）。 设置以秒为单位。
-   `lifetime_max` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 字典在内存中的最 [生存期](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md)，之后ClickHouse尝试重新加载字典（如果 `invalidate_query` 已设置，则仅在字典已更改的情况下）。 设置以秒为单位。
-   `loading_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 加载字典的开始时间。
-   `last_successful_update_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 加载或更新字典的结束时间。帮助监视外部来源的某些故障并调查原因。
-   `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — 字典加载的持续时间。
-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — 如果无法创建字典，则在创建或重新加载字典时发生的错误的文本。

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

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/dictionaries) <!--hide-->
