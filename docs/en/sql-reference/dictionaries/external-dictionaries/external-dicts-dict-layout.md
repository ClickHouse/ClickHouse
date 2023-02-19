---
slug: /en/sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout
sidebar_position: 41
sidebar_label: Storing Dictionaries in Memory
---
import CloudDetails from '@site/docs/en/sql-reference/dictionaries/external-dictionaries/_snippet_dictionary_in_cloud.md';

# Storing Dictionaries in Memory

There are a variety of ways to store dictionaries in memory.

We recommend [flat](#flat), [hashed](#dicts-external_dicts_dict_layout-hashed) and [complex_key_hashed](#complex-key-hashed), which provide optimal processing speed.

Caching is not recommended because of potentially poor performance and difficulties in selecting optimal parameters. Read more in the section [cache](#cache).

There are several ways to improve dictionary performance:

-   Call the function for working with the dictionary after `GROUP BY`.
-   Mark attributes to extract as injective. An attribute is called injective if different attribute values correspond to different keys. So when `GROUP BY` uses a function that fetches an attribute value by the key, this function is automatically taken out of `GROUP BY`.

ClickHouse generates an exception for errors with dictionaries. Examples of errors:

-   The dictionary being accessed could not be loaded.
-   Error querying a `cached` dictionary.

You can view the list of dictionaries and their statuses in the [system.dictionaries](../../../operations/system-tables/dictionaries.md) table.

<CloudDetails />

The configuration looks like this:

``` xml
<clickhouse>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</clickhouse>
```

Corresponding [DDL-query](../../../sql-reference/statements/create/dictionary.md):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

Dictionaries without word `complex-key*` in a layout have a key with [UInt64](../../../sql-reference/data-types/int-uint.md) type, `complex-key*` dictionaries have a composite key (complex, with arbitrary types).

[UInt64](../../../sql-reference/data-types/int-uint.md) keys in XML dictionaries are defined with `<id>` tag.

Configuration example (column key_column has UInt64 type):
```xml
...
<structure>
    <id>
        <name>key_column</name>
    </id>
...
```

Composite `complex` keys XML dictionaries are defined `<key>` tag.

Configuration example of a composite key (key has one element with [String](../../../sql-reference/data-types/string.md) type):
```xml
...
<structure>
    <key>
        <attribute>
            <name>country_code</name>
            <type>String</type>
        </attribute>
    </key>
...
```

## Ways to Store Dictionaries in Memory

-   [flat](#flat)
-   [hashed](#dicts-external_dicts_dict_layout-hashed)
-   [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [complex_key_hashed](#complex-key-hashed)
-   [complex_key_sparse_hashed](#complex-key-sparse-hashed)
-   [hashed_array](#dicts-external_dicts_dict_layout-hashed-array)
-   [complex_key_hashed_array](#complex-key-hashed-array)
-   [range_hashed](#range-hashed)
-   [complex_key_range_hashed](#complex-key-range-hashed)
-   [cache](#cache)
-   [complex_key_cache](#complex-key-cache)
-   [ssd_cache](#ssd-cache)
-   [complex_key_ssd_cache](#complex-key-ssd-cache)
-   [direct](#direct)
-   [complex_key_direct](#complex-key-direct)
-   [ip_trie](#ip-trie)

### flat

The dictionary is completely stored in memory in the form of flat arrays. How much memory does the dictionary use? The amount is proportional to the size of the largest key (in space used).

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type and the value is limited to `max_array_size` (by default — 500,000). If a larger key is discovered when creating the dictionary, ClickHouse throws an exception and does not create the dictionary. Dictionary flat arrays initial size is controlled by `initial_array_size` setting (by default — 1024).

All types of sources are supported. When updating, data (from a file or from a table) is read in it entirety.

This method provides the best performance among all available methods of storing the dictionary.

Configuration example:

``` xml
<layout>
  <flat>
    <initial_array_size>50000</initial_array_size>
    <max_array_size>5000000</max_array_size>
  </flat>
</layout>
```

or

``` sql
LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
```

### hashed

The dictionary is completely stored in memory in the form of a hash table. The dictionary can contain any number of elements with any identifiers In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

Configuration example:

``` xml
<layout>
  <hashed />
</layout>
```

or

``` sql
LAYOUT(HASHED())
```

If `shards` greater then 1 (default is `1`) the dictionary will load data in parallel, useful if you have huge amount of elements in one dictionary.

Configuration example:

``` xml
<layout>
  <hashed>
    <shards>10</shards>
    <!-- Size of the backlog for blocks in parallel queue.

         Since the bottleneck in parallel loading is rehash, and so to avoid
         stalling because of thread is doing rehash, you need to have some
         backlog.

         10000 is good balance between memory and speed.
         Even for 10e10 elements and can handle all the load without starvation. -->
    <shard_load_queue_backlog>10000</shard_load_queue_backlog>
  </hashed>
</layout>
```

or

``` sql
LAYOUT(HASHED(SHARDS 10 [SHARD_LOAD_QUEUE_BACKLOG 10000]))
```

### sparse_hashed

Similar to `hashed`, but uses less memory in favor more CPU usage.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.

Configuration example:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

or

``` sql
LAYOUT(SPARSE_HASHED())
```

It is also possible to use `shards` for this type of dictionary, and again it is more important for `sparse_hashed` then for `hashed`, since `sparse_hashed` is slower.

### complex_key_hashed

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `hashed`.

Configuration example:

``` xml
<layout>
  <complex_key_hashed>
    <shards>1</shards>
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
  </complex_key_hashed>
</layout>
```

or

``` sql
LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000]))
```

### complex_key_sparse_hashed

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed).

Configuration example:

``` xml
<layout>
  <complex_key_sparse_hashed>
    <shards>1</shards>
  </complex_key_sparse_hashed>
</layout>
```

or

``` sql
LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000]))
```

### hashed_array

The dictionary is completely stored in memory. Each attribute is stored in an array. The key attribute is stored in the form of a hashed table where value is an index in the attributes array. The dictionary can contain any number of elements with any identifiers. In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

Configuration example:

``` xml
<layout>
  <hashed_array>
  </hashed_array>
</layout>
```

or

``` sql
LAYOUT(HASHED_ARRAY())
```

### complex_key_hashed_array

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to [hashed_array](#dicts-external_dicts_dict_layout-hashed-array).

Configuration example:

``` xml
<layout>
  <complex_key_hashed_array />
</layout>
```

or

``` sql
LAYOUT(COMPLEX_KEY_HASHED_ARRAY())
```

### range_hashed

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.
This storage method works the same way as hashed and allows using date/time (arbitrary numeric type) ranges in addition to the key.

Example: The table contains discounts for each advertiser in the format:

``` text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

To use a sample for date ranges, define the `range_min` and `range_max` elements in the [structure](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). These elements must contain elements `name` and `type` (if `type` is not specified, the default type will be used - Date). `type` can be any numeric type (Date / DateTime / UInt64 / Int32 / others).

:::warning
Values of `range_min` and `range_max` should fit in `Int64` type.
:::

Example:

``` xml
<layout>
    <range_hashed>
        <!-- Strategy for overlapping ranges (min/max). Default: min (return a matching range with the min(range_min -> range_max) value) -->
        <range_lookup_strategy>min</range_lookup_strategy>
    </range_hashed>
</layout>
<structure>
    <id>
        <name>advertiser_id</name>
    </id>
    <range_min>
        <name>discount_start_date</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>discount_end_date</name>
        <type>Date</type>
    </range_max>
    ...
```

or

``` sql
CREATE DICTIONARY discounts_dict (
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Date,
    amount Float64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'discounts'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN discount_start_date MAX discount_end_date)
```

To work with these dictionaries, you need to pass an additional argument to the `dictGet` function, for which a range is selected:

``` sql
dictGet('dict_name', 'attr_name', id, date)
```
Query example:

``` sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

This function returns the value for the specified `id`s and the date range that includes the passed date.

Details of the algorithm:

-   If the `id` is not found or a range is not found for the `id`, it returns the default value of the attribute's type.
-   If there are overlapping ranges and `range_lookup_strategy=min`, it returns a matching range with minimal `range_min`, if several ranges found, it returns a range with minimal `range_max`, if again several ranges found (several ranges had the same `range_min` and `range_max` it returns a random range of them.
-   If there are overlapping ranges and `range_lookup_strategy=max`, it returns a matching range with maximal `range_min`, if several ranges found, it returns a range with maximal `range_max`, if again several ranges found (several ranges had the same `range_min` and `range_max` it returns a random range of them.
-   If the `range_max` is `NULL`, the range is open. `NULL` is treated as maximal possible value. For the `range_min` `1970-01-01` or `0` (-MAX_INT) can be used as the open value.

Configuration example:

``` xml
<clickhouse>
    <dictionary>
        ...

        <layout>
            <range_hashed />
        </layout>

        <structure>
            <id>
                <name>Abcdef</name>
            </id>
            <range_min>
                <name>StartTimeStamp</name>
                <type>UInt64</type>
            </range_min>
            <range_max>
                <name>EndTimeStamp</name>
                <type>UInt64</type>
            </range_max>
            <attribute>
                <name>XXXType</name>
                <type>String</type>
                <null_value />
            </attribute>
        </structure>

    </dictionary>
</clickhouse>
```

or

``` sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

Configuration example with overlapping ranges and open ranges:

```sql
CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

SELECT * FROM discounts ORDER BY advertiser_id, discount_start_date;
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│             1 │          2015-01-01 │              ᴺᵁᴸᴸ │    0.1 │
│             1 │          2015-01-15 │              ᴺᵁᴸᴸ │    0.2 │
│             2 │          2015-01-01 │        2015-01-15 │    0.3 │
│             2 │          2015-01-04 │        2015-01-10 │    0.4 │
│             3 │          1970-01-01 │        2015-01-15 │    0.5 │
│             3 │          1970-01-01 │        2015-01-10 │    0.6 │
└───────────────┴─────────────────────┴───────────────────┴────────┘

-- RANGE_LOOKUP_STRATEGY 'max'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.2 │ -- two ranges are matching, range_min 2015-01-15 (0.2) is bigger than 2015-01-01 (0.1)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.4 │ -- two ranges are matching, range_min 2015-01-04 (0.4) is bigger than 2015-01-01 (0.3)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.5 │ -- two ranges are matching, range_min are equal, 2015-01-15 (0.5) is bigger than 2015-01-10 (0.6)
└─────┘

DROP DICTIONARY discounts_dict;

-- RANGE_LOOKUP_STRATEGY 'min'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.1 │ -- two ranges are matching, range_min 2015-01-01 (0.1) is less than 2015-01-15 (0.2)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.3 │ -- two ranges are matching, range_min 2015-01-01 (0.3) is less than 2015-01-04 (0.4)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.6 │ -- two ranges are matching, range_min are equal, 2015-01-10 (0.6) is less than 2015-01-15 (0.5)
└─────┘
```

### complex_key_range_hashed

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values (see [range_hashed](#range-hashed)). This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md).

Configuration example:

``` sql
CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  CountryKey String,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);
```

### cache

The dictionary is stored in a cache that has a fixed number of cells. These cells contain frequently used elements.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.

When searching for a dictionary, the cache is searched first. For each block of data, all keys that are not found in the cache or are outdated are requested from the source using `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. The received data is then written to the cache.

If keys are not found in dictionary, then update cache task is created and added into update queue. Update queue properties can be controlled with settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

For cache dictionaries, the expiration [lifetime](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) of data in the cache can be set. If more time than `lifetime` has passed since loading the data in a cell, the cell’s value is not used and key becomes expired. The key is re-requested the next time it needs to be used. This behaviour can be configured with setting `allow_read_expired_keys`.

This is the least effective of all the ways to store dictionaries. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary performs well only when the hit rates are high enough (recommended 99% and higher). You can view the average hit rate in the [system.dictionaries](../../../operations/system-tables/dictionaries.md) table.

If setting `allow_read_expired_keys` is set to 1, by default 0. Then dictionary can support asynchronous updates. If a client requests keys and all of them are in cache, but some of them are expired, then dictionary will return expired keys for a client and request them asynchronously from the source.

To improve cache performance, use a subquery with `LIMIT`, and call the function with the dictionary externally.

All types of sources are supported.

Example of settings:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
        <!-- Allows to read expired keys. -->
        <allow_read_expired_keys>0</allow_read_expired_keys>
        <!-- Max size of update queue. -->
        <max_update_queue_size>100000</max_update_queue_size>
        <!-- Max timeout in milliseconds for push update task into queue. -->
        <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
        <!-- Max wait timeout in milliseconds for update task to complete. -->
        <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
        <!-- Max threads for cache dictionary update. -->
        <max_threads_for_updates>4</max_threads_for_updates>
    </cache>
</layout>
```

or

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

Set a large enough cache size. You need to experiment to select the number of cells:

1.  Set some value.
2.  Run queries until the cache is completely full.
3.  Assess memory consumption using the `system.dictionaries` table.
4.  Increase or decrease the number of cells until the required memory consumption is reached.

:::warning
Do not use ClickHouse as a source, because it is slow to process queries with random reads.
:::

### complex_key_cache

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `cache`.

### ssd_cache

Similar to `cache`, but stores data on SSD and index in RAM. All cache dictionary settings related to update queue can also be applied to SSD cache dictionaries.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.

``` xml
<layout>
    <ssd_cache>
        <!-- Size of elementary read block in bytes. Recommended to be equal to SSD's page size. -->
        <block_size>4096</block_size>
        <!-- Max cache file size in bytes. -->
        <file_size>16777216</file_size>
        <!-- Size of RAM buffer in bytes for reading elements from SSD. -->
        <read_buffer_size>131072</read_buffer_size>
        <!-- Size of RAM buffer in bytes for aggregating elements before flushing to SSD. -->
        <write_buffer_size>1048576</write_buffer_size>
        <!-- Path where cache file will be stored. -->
        <path>/var/lib/clickhouse/user_files/test_dict</path>
    </ssd_cache>
</layout>
```

or

``` sql
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
    PATH '/var/lib/clickhouse/user_files/test_dict'))
```

### complex_key_ssd_cache

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `ssd_cache`.

### direct

The dictionary is not stored in memory and directly goes to the source during the processing of a request.

The dictionary key has the [UInt64](../../../sql-reference/data-types/int-uint.md) type.

All types of [sources](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), except local files, are supported.

Configuration example:

``` xml
<layout>
  <direct />
</layout>
```

or

``` sql
LAYOUT(DIRECT())
```

### complex_key_direct

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `direct`.

### ip_trie

This type of storage is for mapping network prefixes (IP addresses) to metadata such as ASN.

**Example**

Suppose we have a table in ClickHouse that contains our IP prefixes and mappings:

```sql
CREATE TABLE my_ip_addresses (
	prefix String,
	asn UInt32,
	cca2 String
)
ENGINE = MergeTree
PRIMARY KEY prefix;
```

```sql
INSERT INTO my_ip_addresses VALUES
	('202.79.32.0/20', 17501, 'NP'),
    ('2620:0:870::/48', 3856, 'US'),
    ('2a02:6b8:1::/48', 13238, 'RU'),
    ('2001:db8::/32', 65536, 'ZZ')
;
```

Let's define an `ip_trie` dictionary for this table. The `ip_trie` layout requires a composite key:

``` xml
<structure>
    <key>
        <attribute>
            <name>prefix</name>
            <type>String</type>
        </attribute>
    </key>
    <attribute>
            <name>asn</name>
            <type>UInt32</type>
            <null_value />
    </attribute>
    <attribute>
            <name>cca2</name>
            <type>String</type>
            <null_value>??</null_value>
    </attribute>
    ...
</structure>
<layout>
    <ip_trie>
        <!-- Key attribute `prefix` can be retrieved via dictGetString. -->
        <!-- This option increases memory usage. -->
        <access_to_key_from_attributes>true</access_to_key_from_attributes>
    </ip_trie>
</layout>
```

or

``` sql
CREATE DICTIONARY my_ip_trie_dictionary (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'my_ip_addresses'))
LAYOUT(IP_TRIE)
LIFETIME(3600);
```

The key must have only one `String` type attribute that contains an allowed IP prefix. Other types are not supported yet.

For queries, you must use the same functions (`dictGetT` with a tuple) as for dictionaries with composite keys. The syntax is:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

The function takes either `UInt32` for IPv4, or `FixedString(16)` for IPv6. For example:

``` sql
select dictGet('my_ip_trie_dictionary', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

Other types are not supported yet. The function returns the attribute for the prefix that corresponds to this IP address. If there are overlapping prefixes, the most specific one is returned.

Data must completely fit into RAM.

## Related Content

- [Using dictionaries to accelerate queries](https://clickhouse.com/blog/faster-queries-dictionaries-clickhouse)
