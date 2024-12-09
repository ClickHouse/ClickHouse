---
slug: /en/sql-reference/dictionaries
sidebar_label: Defining Dictionaries
sidebar_position: 35
---

import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';
import CloudDetails from '@site/docs/en/sql-reference/dictionaries/_snippet_dictionary_in_cloud.md';

# Dictionaries

A dictionary is a mapping (`key -> attributes`) that is convenient for various types of reference lists.

ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a `JOIN` with reference tables.

ClickHouse supports:

- Dictionaries with a [set of functions](../../sql-reference/functions/ext-dict-functions.md).
- [Embedded dictionaries](#embedded-dictionaries) with a specific [set of functions](../../sql-reference/functions/ym-dict-functions.md).


:::tip Tutorial
If you are getting started with Dictionaries in ClickHouse we have a tutorial that covers that topic.  Take a look [here](/docs/en/tutorial.md).
:::

You can add your own dictionaries from various data sources. The source for a dictionary can be a ClickHouse table, a local text or executable file, an HTTP(s) resource, or another DBMS. For more information, see “[Dictionary Sources](#dictionary-sources)”.

ClickHouse:

- Fully or partially stores dictionaries in RAM.
- Periodically updates dictionaries and dynamically loads missing values. In other words, dictionaries can be loaded dynamically.
- Allows creating dictionaries with xml files or [DDL queries](../../sql-reference/statements/create/dictionary.md).

The configuration of dictionaries can be located in one or more xml-files. The path to the configuration is specified in the [dictionaries_config](../../operations/server-configuration-parameters/settings.md#dictionaries_config) parameter.

Dictionaries can be loaded at server startup or at first use, depending on the [dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load) setting.

The [dictionaries](../../operations/system-tables/dictionaries.md#system_tables-dictionaries) system table contains information about dictionaries configured at server. For each dictionary you can find there:

- Status of the dictionary.
- Configuration parameters.
- Metrics like amount of RAM allocated for the dictionary or a number of queries since the dictionary was successfully loaded.

<CloudDetails />

## Creating a dictionary with a DDL query {#creating-a-dictionary-with-a-ddl-query}

Dictionaries can be created with [DDL queries](../../sql-reference/statements/create/dictionary.md), and this is the recommended method because with DDL created dictionaries:
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
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

You can [configure](#configuring-a-dictionary) any number of dictionaries in the same file.


:::note
You can convert values for a small dictionary by describing it in a `SELECT` query (see the [transform](../../sql-reference/functions/other-functions.md) function). This functionality is not related to dictionaries.
:::

## Configuring a Dictionary

<CloudDetails />

If dictionary is configured using xml file, than dictionary configuration has the following structure:

``` xml
<dictionary>
    <name>dict_name</name>

    <structure>
      <!-- Complex key configuration -->
    </structure>

    <source>
      <!-- Source configuration -->
    </source>

    <layout>
      <!-- Memory layout configuration -->
    </layout>

    <lifetime>
      <!-- Lifetime of dictionary in memory -->
    </lifetime>
</dictionary>
```

Corresponding [DDL-query](../../sql-reference/statements/create/dictionary.md) has the following structure:

``` sql
CREATE DICTIONARY dict_name
(
    ... -- attributes
)
PRIMARY KEY ... -- complex or single key configuration
SOURCE(...) -- Source configuration
LAYOUT(...) -- Memory layout configuration
LIFETIME(...) -- Lifetime of dictionary in memory
```

## Storing Dictionaries in Memory

There are a variety of ways to store dictionaries in memory.

We recommend [flat](#flat), [hashed](#hashed) and [complex_key_hashed](#complex_key_hashed), which provide optimal processing speed.

Caching is not recommended because of potentially poor performance and difficulties in selecting optimal parameters. Read more in the section [cache](#cache).

There are several ways to improve dictionary performance:

- Call the function for working with the dictionary after `GROUP BY`.
- Mark attributes to extract as injective. An attribute is called injective if different attribute values correspond to different keys. So when `GROUP BY` uses a function that fetches an attribute value by the key, this function is automatically taken out of `GROUP BY`.

ClickHouse generates an exception for errors with dictionaries. Examples of errors:

- The dictionary being accessed could not be loaded.
- Error querying a `cached` dictionary.

You can view the list of dictionaries and their statuses in the [system.dictionaries](../../operations/system-tables/dictionaries.md) table.

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

Corresponding [DDL-query](../../sql-reference/statements/create/dictionary.md):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

Dictionaries without word `complex-key*` in a layout have a key with [UInt64](../../sql-reference/data-types/int-uint.md) type, `complex-key*` dictionaries have a composite key (complex, with arbitrary types).

[UInt64](../../sql-reference/data-types/int-uint.md) keys in XML dictionaries are defined with `<id>` tag.

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

Configuration example of a composite key (key has one element with [String](../../sql-reference/data-types/string.md) type):
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

- [flat](#flat)
- [hashed](#hashed)
- [sparse_hashed](#sparse_hashed)
- [complex_key_hashed](#complex_key_hashed)
- [complex_key_sparse_hashed](#complex_key_sparse_hashed)
- [hashed_array](#hashed_array)
- [complex_key_hashed_array](#complex_key_hashed_array)
- [range_hashed](#range_hashed)
- [complex_key_range_hashed](#complex_key_range_hashed)
- [cache](#cache)
- [complex_key_cache](#complex_key_cache)
- [ssd_cache](#ssd_cache)
- [complex_key_ssd_cache](#complex_key_ssd_cache)
- [direct](#direct)
- [complex_key_direct](#complex_key_direct)
- [ip_trie](#ip_trie)

### flat

The dictionary is completely stored in memory in the form of flat arrays. How much memory does the dictionary use? The amount is proportional to the size of the largest key (in space used).

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type and the value is limited to `max_array_size` (by default — 500,000). If a larger key is discovered when creating the dictionary, ClickHouse throws an exception and does not create the dictionary. Dictionary flat arrays initial size is controlled by `initial_array_size` setting (by default — 1024).

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

The dictionary is completely stored in memory in the form of a hash table. The dictionary can contain any number of elements with any identifiers. In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.

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

Configuration example:

``` xml
<layout>
  <hashed>
    <!-- If shards greater then 1 (default is `1`) the dictionary will load
         data in parallel, useful if you have huge amount of elements in one
         dictionary. -->
    <shards>10</shards>

    <!-- Size of the backlog for blocks in parallel queue.

         Since the bottleneck in parallel loading is rehash, and so to avoid
         stalling because of thread is doing rehash, you need to have some
         backlog.

         10000 is good balance between memory and speed.
         Even for 10e10 elements and can handle all the load without starvation. -->
    <shard_load_queue_backlog>10000</shard_load_queue_backlog>

    <!-- Maximum load factor of the hash table, with greater values, the memory
         is utilized more efficiently (less memory is wasted) but read/performance
         may deteriorate.

         Valid values: [0.5, 0.99]
         Default: 0.5 -->
    <max_load_factor>0.5</max_load_factor>
  </hashed>
</layout>
```

or

``` sql
LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

### sparse_hashed

Similar to `hashed`, but uses less memory in favor more CPU usage.

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.

Configuration example:

``` xml
<layout>
  <sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </sparse_hashed>
</layout>
```

or

``` sql
LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

It is also possible to use `shards` for this type of dictionary, and again it is more important for `sparse_hashed` then for `hashed`, since `sparse_hashed` is slower.

### complex_key_hashed

This type of storage is for use with composite [keys](#dictionary-key-and-fields). Similar to `hashed`.

Configuration example:

``` xml
<layout>
  <complex_key_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_hashed>
</layout>
```

or

``` sql
LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

### complex_key_sparse_hashed

This type of storage is for use with composite [keys](#dictionary-key-and-fields). Similar to [sparse_hashed](#sparse_hashed).

Configuration example:

``` xml
<layout>
  <complex_key_sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_sparse_hashed>
</layout>
```

or

``` sql
LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

### hashed_array

The dictionary is completely stored in memory. Each attribute is stored in an array. The key attribute is stored in the form of a hashed table where value is an index in the attributes array. The dictionary can contain any number of elements with any identifiers. In practice, the number of keys can reach tens of millions of items.

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.

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
LAYOUT(HASHED_ARRAY([SHARDS 1]))
```

### complex_key_hashed_array

This type of storage is for use with composite [keys](#dictionary-key-and-fields). Similar to [hashed_array](#hashed_array).

Configuration example:

``` xml
<layout>
  <complex_key_hashed_array />
</layout>
```

or

``` sql
LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
```

### range_hashed

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values.

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.
This storage method works the same way as hashed and allows using date/time (arbitrary numeric type) ranges in addition to the key.

Example: The table contains discounts for each advertiser in the format:

``` text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

To use a sample for date ranges, define the `range_min` and `range_max` elements in the [structure](#dictionary-key-and-fields). These elements must contain elements `name` and `type` (if `type` is not specified, the default type will be used - Date). `type` can be any numeric type (Date / DateTime / UInt64 / Int32 / others).

:::note
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

- If the `id` is not found or a range is not found for the `id`, it returns the default value of the attribute's type.
- If there are overlapping ranges and `range_lookup_strategy=min`, it returns a matching range with minimal `range_min`, if several ranges found, it returns a range with minimal `range_max`, if again several ranges found (several ranges had the same `range_min` and `range_max` it returns a random range of them.
- If there are overlapping ranges and `range_lookup_strategy=max`, it returns a matching range with maximal `range_min`, if several ranges found, it returns a range with maximal `range_max`, if again several ranges found (several ranges had the same `range_min` and `range_max` it returns a random range of them.
- If the `range_max` is `NULL`, the range is open. `NULL` is treated as maximal possible value. For the `range_min` `1970-01-01` or `0` (-MAX_INT) can be used as the open value.

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

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values (see [range_hashed](#range_hashed)). This type of storage is for use with composite [keys](#dictionary-key-and-fields).

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

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.

When searching for a dictionary, the cache is searched first. For each block of data, all keys that are not found in the cache or are outdated are requested from the source using `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. The received data is then written to the cache.

If keys are not found in dictionary, then update cache task is created and added into update queue. Update queue properties can be controlled with settings `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

For cache dictionaries, the expiration [lifetime](#refreshing-dictionary-data-using-lifetime) of data in the cache can be set. If more time than `lifetime` has passed since loading the data in a cell, the cell’s value is not used and key becomes expired. The key is re-requested the next time it needs to be used. This behaviour can be configured with setting `allow_read_expired_keys`.

This is the least effective of all the ways to store dictionaries. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary performs well only when the hit rates are high enough (recommended 99% and higher). You can view the average hit rate in the [system.dictionaries](../../operations/system-tables/dictionaries.md) table.

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

:::note
Do not use ClickHouse as a source, because it is slow to process queries with random reads.
:::

### complex_key_cache

This type of storage is for use with composite [keys](#dictionary-key-and-fields). Similar to `cache`.

### ssd_cache

Similar to `cache`, but stores data on SSD and index in RAM. All cache dictionary settings related to update queue can also be applied to SSD cache dictionaries.

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.

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

This type of storage is for use with composite [keys](#dictionary-key-and-fields). Similar to `ssd_cache`.

### direct

The dictionary is not stored in memory and directly goes to the source during the processing of a request.

The dictionary key has the [UInt64](../../sql-reference/data-types/int-uint.md) type.

All types of [sources](#dictionary-sources), except local files, are supported.

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

This type of storage is for use with composite [keys](#dictionary-key-and-fields). Similar to `direct`.

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

The syntax is:

``` sql
dictGetT('dict_name', 'attr_name', ip)
```

The function takes either `UInt32` for IPv4, or `FixedString(16)` for IPv6. For example:

``` sql
SELECT dictGet('my_ip_trie_dictionary', 'cca2', toIPv4('202.79.32.10')) AS result;

┌─result─┐
│ NP     │
└────────┘


SELECT dictGet('my_ip_trie_dictionary', 'asn', IPv6StringToNum('2001:db8::1')) AS result;

┌─result─┐
│  65536 │
└────────┘


SELECT dictGet('my_ip_trie_dictionary', ('asn', 'cca2'), IPv6StringToNum('2001:db8::1')) AS result;

┌─result───────┐
│ (65536,'ZZ') │
└──────────────┘
```

Other types are not supported yet. The function returns the attribute for the prefix that corresponds to this IP address. If there are overlapping prefixes, the most specific one is returned.

Data must completely fit into RAM.

## Refreshing dictionary data using LIFETIME

ClickHouse periodically updates dictionaries based on the `LIFETIME` tag (defined in seconds). `LIFETIME` is the update interval for fully downloaded dictionaries and the invalidation interval for cached dictionaries.

During updates, the old version of a dictionary can still be queried. Dictionary updates (other than when loading the dictionary for first use) do not block queries. If an error occurs during an update, the error is written to the server log and queries can continue using the old version of the dictionary. If a dictionary update is successful, the old version of the dictionary is replaced atomically.

Example of settings:

<CloudDetails />

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

When updating the dictionaries, the ClickHouse server applies different logic depending on the type of [source](#dictionary-sources):

- For a text file, it checks the time of modification. If the time differs from the previously recorded time, the dictionary is updated.
- For MySQL source, the time of modification is checked using a `SHOW TABLE STATUS` query (in case of MySQL 8 you need to disable meta-information caching in MySQL by `set global information_schema_stats_expiry=0`).
- Dictionaries from other sources are updated every time by default.

For other sources (ODBC, PostgreSQL, ClickHouse, etc), you can set up a query that will update the dictionaries only if they really changed, rather than each time. To do this, follow these steps:

- The dictionary table must have a field that always changes when the source data is updated.
- The settings of the source must specify a query that retrieves the changing field. The ClickHouse server interprets the query result as a row, and if this row has changed relative to its previous state, the dictionary is updated. Specify the query in the `<invalidate_query>` field in the settings for the [source](#dictionary-sources).

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

For `Cache`, `ComplexKeyCache`, `SSDCache`, and `SSDComplexKeyCache` dictionaries both synchronous and asynchronous updates are supported.

It is also possible for `Flat`, `Hashed`, `ComplexKeyHashed` dictionaries to only request data that was changed after the previous update. If `update_field` is specified as part of the dictionary source configuration, value of the previous update time in seconds will be added to the data request. Depends on source type (Executable, HTTP, MySQL, PostgreSQL, ClickHouse, or ODBC) different logic will be applied to `update_field` before request data from an external source.

- If the source is HTTP then `update_field` will be added as a query parameter with the last update time as the parameter value.
- If the source is Executable then `update_field` will be added as an executable script argument with the last update time as the argument value.
- If the source is ClickHouse, MySQL, PostgreSQL, ODBC there will be an additional part of `WHERE`, where `update_field` is compared as greater or equal with the last update time.
    - Per default, this `WHERE`-condition is checked at the highest level of the SQL-Query. Alternatively, the condition can be checked in any other `WHERE`-clause within the query using the `{condition}`-keyword. Example:
    ```sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

If `update_field` option is set, additional option `update_lag` can be set. Value of `update_lag` option is subtracted from previous update time before request updated data.

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

## Dictionary Sources

<CloudDetails />

A dictionary can be connected to ClickHouse from many different sources.

If the dictionary is configured using an xml-file, the configuration looks like this:

``` xml
<clickhouse>
  <dictionary>
    ...
    <source>
      <source_type>
        <!-- Source configuration -->
      </source_type>
    </source>
    ...
  </dictionary>
  ...
</clickhouse>
```

In case of [DDL-query](../../sql-reference/statements/create/dictionary.md), the configuration described above will look like:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- Source configuration
...
```

The source is configured in the `source` section.

For source types [Local file](#local-file), [Executable file](#executable-file), [HTTP(s)](#https), [ClickHouse](#clickhouse)
optional settings are available:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
  <settings>
      <format_csv_allow_single_quotes>0</format_csv_allow_single_quotes>
  </settings>
</source>
```

or

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

Types of sources (`source_type`):

- [Local file](#local-file)
- [Executable File](#executable-file)
- [Executable Pool](#executable-pool)
- [HTTP(S)](#https)
- DBMS
    - [ODBC](#odbc)
    - [MySQL](#mysql)
    - [ClickHouse](#clickhouse)
    - [MongoDB](#mongodb)
    - [Redis](#redis)
    - [Cassandra](#cassandra)
    - [PostgreSQL](#postgresql)

### Local File

Example of settings:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

or

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

Setting fields:

- `path` – The absolute path to the file.
- `format` – The file format. All the formats described in [Formats](../../interfaces/formats.md#formats) are supported.

When a dictionary with source `FILE` is created via DDL command (`CREATE DICTIONARY ...`), the source file needs to be located in the `user_files` directory to prevent DB users from accessing arbitrary files on the ClickHouse node.

**See Also**

- [Dictionary function](../../sql-reference/table-functions/dictionary.md#dictionary-function)

### Executable File

Working with executable files depends on [how the dictionary is stored in memory](#storing-dictionaries-in-memory). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request to the executable file’s STDIN. Otherwise, ClickHouse starts the executable file and treats its output as dictionary data.

Example of settings:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
        <implicit_key>false</implicit_key>
    </executable>
</source>
```

Setting fields:

- `command` — The absolute path to the executable file, or the file name (if the command's directory is in the `PATH`).
- `format` — The file format. All the formats described in [Formats](../../interfaces/formats.md#formats) are supported.
- `command_termination_timeout` — The executable script should contain a main read-write loop. After the dictionary is destroyed, the pipe is closed, and the executable file will have `command_termination_timeout` seconds to shutdown before ClickHouse will send a SIGTERM signal to the child process. `command_termination_timeout` is specified in seconds. Default value is 10. Optional parameter.
- `command_read_timeout` - Timeout for reading data from command stdout in milliseconds. Default value 10000. Optional parameter.
- `command_write_timeout` - Timeout for writing data to command stdin in milliseconds. Default value 10000. Optional parameter.
- `implicit_key` — The executable source file can return only values, and the correspondence to the requested keys is determined implicitly — by the order of rows in the result. Default value is false.
- `execute_direct` - If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Additional script arguments can be specified using a whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `0`. Optional parameter.
- `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Optional. Default value is `false`.

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled; otherwise, the DB user would be able to execute arbitrary binaries on the ClickHouse node.

### Executable Pool

Executable pool allows loading data from pool of processes. This source does not work with dictionary layouts that need to load all data from source. Executable pool works if the dictionary [is stored](#ways-to-store-dictionaries-in-memory) using `cache`, `complex_key_cache`, `ssd_cache`, `complex_key_ssd_cache`, `direct`, or `complex_key_direct` layouts.

Executable pool will spawn a pool of processes with the specified command and keep them running until they exit. The program should read data from STDIN while it is available and output the result to STDOUT. It can wait for the next block of data on STDIN. ClickHouse will not close STDIN after processing a block of data, but will pipe another chunk of data when needed. The executable script should be ready for this way of data processing — it should poll STDIN and flush data to STDOUT early.

Example of settings:

``` xml
<source>
    <executable_pool>
        <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
        <format>TabSeparated</format>
        <pool_size>10</pool_size>
        <max_command_execution_time>10<max_command_execution_time>
        <implicit_key>false</implicit_key>
    </executable_pool>
</source>
```

Setting fields:

- `command` — The absolute path to the executable file, or the file name (if the program directory is written to `PATH`).
- `format` — The file format. All the formats described in “[Formats](../../interfaces/formats.md#formats)” are supported.
- `pool_size` — Size of pool. If 0 is specified as `pool_size` then there is no pool size restrictions. Default value is `16`.
- `command_termination_timeout` — executable script should contain main read-write loop. After dictionary is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown, before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is 10. Optional parameter.
- `max_command_execution_time` — Maximum executable script command execution time for processing block of data. Specified in seconds. Default value is 10. Optional parameter.
- `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000. Optional parameter.
- `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000. Optional parameter.
- `implicit_key` — The executable source file can return only values, and the correspondence to the requested keys is determined implicitly — by the order of rows in the result. Default value is false. Optional parameter.
- `execute_direct` - If `execute_direct` = `1`, then `command` will be searched inside user_scripts folder specified by [user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Additional script arguments can be specified using whitespace separator. Example: `script_name arg1 arg2`. If `execute_direct` = `0`, `command` is passed as argument for `bin/sh -c`. Default value is `1`. Optional parameter.
- `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Optional. Default value is `false`.

That dictionary source can be configured only via XML configuration. Creating dictionaries with executable source via DDL is disabled, otherwise, the DB user would be able to execute arbitrary binary on ClickHouse node.

### HTTP(S)

Working with an HTTP(S) server depends on [how the dictionary is stored in memory](#storing-dictionaries-in-memory). If the dictionary is stored using `cache` and `complex_key_cache`, ClickHouse requests the necessary keys by sending a request via the `POST` method.

Example of settings:

``` xml
<source>
    <http>
        <url>http://[::1]/os.tsv</url>
        <format>TabSeparated</format>
        <credentials>
            <user>user</user>
            <password>password</password>
        </credentials>
        <headers>
            <header>
                <name>API-KEY</name>
                <value>key</value>
            </header>
        </headers>
    </http>
</source>
```

or

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

In order for ClickHouse to access an HTTPS resource, you must [configure openSSL](../../operations/server-configuration-parameters/settings.md#openssl) in the server configuration.

Setting fields:

- `url` – The source URL.
- `format` – The file format. All the formats described in “[Formats](../../interfaces/formats.md#formats)” are supported.
- `credentials` – Basic HTTP authentication. Optional parameter.
- `user` – Username required for the authentication.
- `password` – Password required for the authentication.
- `headers` – All custom HTTP headers entries used for the HTTP request. Optional parameter.
- `header` – Single HTTP header entry.
- `name` – Identifier name used for the header send on the request.
- `value` – Value set for a specific identifier name.

When creating a dictionary using the DDL command (`CREATE DICTIONARY ...`) remote hosts for HTTP dictionaries are checked against the contents of `remote_url_allow_hosts` section from config to prevent database users to access arbitrary HTTP server.

### DBMS

#### ODBC

You can use this method to connect any database that has an ODBC driver.

Example of settings:

``` xml
<source>
    <odbc>
        <db>DatabaseName</db>
        <table>ShemaName.TableName</table>
        <connection_string>DSN=some_parameters</connection_string>
        <invalidate_query>SQL_QUERY</invalidate_query>
        <query>SELECT id, value_1, value_2 FROM ShemaName.TableName</query>
    </odbc>
</source>
```

or

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Setting fields:

- `db` – Name of the database. Omit it if the database name is set in the `<connection_string>` parameters.
- `table` – Name of the table and schema if exists.
- `connection_string` – Connection string.
- `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Refreshing dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime).
- `query` – The custom query. Optional parameter.

:::note
The `table` and `query` fields cannot be used together. And either one of the `table` or `query` fields must be declared.
:::

ClickHouse receives quoting symbols from ODBC-driver and quote all settings in queries to driver, so it’s necessary to set table name accordingly to table name case in database.

If you have a problems with encodings when using Oracle, see the corresponding [FAQ](/knowledgebase/oracle-odbc) item.

##### Known Vulnerability of the ODBC Dictionary Functionality

:::note
When connecting to the database through the ODBC driver connection parameter `Servername` can be substituted. In this case values of `USERNAME` and `PASSWORD` from `odbc.ini` are sent to the remote server and can be compromised.
:::

**Example of insecure use**

Let’s configure unixODBC for PostgreSQL. Content of `/etc/odbc.ini`:

``` text
[gregtest]
Driver = /usr/lib/psqlodbca.so
Servername = localhost
PORT = 5432
DATABASE = test_db
#OPTION = 3
USERNAME = test
PASSWORD = test
```

If you then make a query such as

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC driver will send values of `USERNAME` and `PASSWORD` from `odbc.ini` to `some-server.com`.

##### Example of Connecting Postgresql

Ubuntu OS.

Installing unixODBC and the ODBC driver for PostgreSQL:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuring `/etc/odbc.ini` (or `~/.odbc.ini` if you signed in under a user that runs ClickHouse):

``` text
    [DEFAULT]
    Driver = myconnection

    [myconnection]
    Description         = PostgreSQL connection to my_db
    Driver              = PostgreSQL Unicode
    Database            = my_db
    Servername          = 127.0.0.1
    UserName            = username
    Password            = password
    Port                = 5432
    Protocol            = 9.3
    ReadOnly            = No
    RowVersioning       = No
    ShowSystemTables    = No
    ConnSettings        =
```

The dictionary configuration in ClickHouse:

``` xml
<clickhouse>
    <dictionary>
        <name>table_name</name>
        <source>
            <odbc>
                <!-- You can specify the following parameters in connection_string: -->
                <!-- DSN=myconnection;UID=username;PWD=password;HOST=127.0.0.1;PORT=5432;DATABASE=my_db -->
                <connection_string>DSN=myconnection</connection_string>
                <table>postgresql_table</table>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>some_column</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

or

``` sql
CREATE DICTIONARY table_name (
    id UInt64,
    some_column UInt64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(ODBC(connection_string 'DSN=myconnection' table 'postgresql_table'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360)
```

You may need to edit `odbc.ini` to specify the full path to the library with the driver `DRIVER=/usr/local/lib/psqlodbcw.so`.

##### Example of Connecting MS SQL Server

Ubuntu OS.

Installing the ODBC driver for connecting to MS SQL:

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuring the driver:

```bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    # test TDS connection
    $ sqsh -S MSSQL -D database -U user -P password


    $ cat /etc/odbcinst.ini

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat /etc/odbc.ini
    # $ cat ~/.odbc.ini # if you signed in under a user that runs ClickHouse

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433


    # (optional) test ODBC connection (to use isql-tool install the [unixodbc](https://packages.debian.org/sid/unixodbc)-package)
    $ isql -v MSSQL "user" "password"
```

Remarks:
- to determine the earliest TDS version that is supported by a particular SQL Server version, refer to the product documentation or look at [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

Configuring the dictionary in ClickHouse:

``` xml
<clickhouse>
    <dictionary>
        <name>test</name>
        <source>
            <odbc>
                <table>dict</table>
                <connection_string>DSN=MSSQL;UID=test;PWD=test</connection_string>
            </odbc>
        </source>

        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>

        <layout>
            <flat />
        </layout>

        <structure>
            <id>
                <name>k</name>
            </id>
            <attribute>
                <name>s</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

or

``` sql
CREATE DICTIONARY test (
    k UInt64,
    s String DEFAULT ''
)
PRIMARY KEY k
SOURCE(ODBC(table 'dict' connection_string 'DSN=MSSQL;UID=test;PWD=test'))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 360)
```

#### Mysql

Example of settings:

``` xml
<source>
  <mysql>
      <port>3306</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <replica>
          <host>example01-1</host>
          <priority>1</priority>
      </replica>
      <replica>
          <host>example01-2</host>
          <priority>1</priority>
      </replica>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

or

``` sql
SOURCE(MYSQL(
    port 3306
    user 'clickhouse'
    password 'qwerty'
    replica(host 'example01-1' priority 1)
    replica(host 'example01-2' priority 1)
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Setting fields:

- `port` – The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).

- `user` – Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

- `password` – Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).

- `replica` – Section of replica configurations. There can be multiple sections.

        - `replica/host` – The MySQL host.
        - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.

- `db` – Name of the database.

- `table` – Name of the table.

- `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` clause in MySQL, for example, `id > 10 AND id < 20`. Optional parameter.

- `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Refreshing dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime).

- `fail_on_connection_loss` – The configuration parameter that controls behavior of the server on connection loss. If `true`, an exception is thrown immediately if the connection between client and server was lost. If `false`, the ClickHouse server retries to execute the query three times before throwing an exception. Note that retrying leads to increased response times. Default value: `false`.

- `query` – The custom query. Optional parameter.

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::

:::note
There is no explicit parameter `secure`. When establishing an SSL-connection security is mandatory.
:::

MySQL can be connected to on a local host via sockets. To do this, set `host` and `socket`.

Example of settings:

``` xml
<source>
  <mysql>
      <host>localhost</host>
      <socket>/path/to/socket/file.sock</socket>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
	  <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

or

``` sql
SOURCE(MYSQL(
    host 'localhost'
    socket '/path/to/socket/file.sock'
    user 'clickhouse'
    password 'qwerty'
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
	query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

#### ClickHouse

Example of settings:

``` xml
<source>
    <clickhouse>
        <host>example01-01-1</host>
        <port>9000</port>
        <user>default</user>
        <password></password>
        <db>default</db>
        <table>ids</table>
        <where>id=10</where>
        <secure>1</secure>
		<query>SELECT id, value_1, value_2 FROM default.ids</query>
    </clickhouse>
</source>
```

or

``` sql
SOURCE(CLICKHOUSE(
    host 'example01-01-1'
    port 9000
    user 'default'
    password ''
    db 'default'
    table 'ids'
    where 'id=10'
    secure 1
	query 'SELECT id, value_1, value_2 FROM default.ids'
));
```

Setting fields:

- `host` – The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distributed](../../engines/table-engines/special/distributed.md) table and enter it in subsequent configurations.
- `port` – The port on the ClickHouse server.
- `user` – Name of the ClickHouse user.
- `password` – Password of the ClickHouse user.
- `db` – Name of the database.
- `table` – Name of the table.
- `where` – The selection criteria. May be omitted.
- `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Refreshing dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime).
- `secure` - Use ssl for connection.
- `query` – The custom query. Optional parameter.

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::

#### MongoDB

Example of settings:

``` xml
<source>
    <mongodb>
        <host>localhost</host>
        <port>27017</port>
        <user></user>
        <password></password>
        <db>test</db>
        <collection>dictionary_source</collection>
        <options>ssl=true</options>
    </mongodb>
</source>
```

or

``` xml
<source>
    <mongodb>
        <uri>mongodb://localhost:27017/test?ssl=true</uri>
        <collection>dictionary_source</collection>
    </mongodb>
</source>
```

or

``` sql
SOURCE(MONGODB(
    host 'localhost'
    port 27017
    user ''
    password ''
    db 'test'
    collection 'dictionary_source'
    options 'ssl=true'
))
```

Setting fields:

- `host` – The MongoDB host.
- `port` – The port on the MongoDB server.
- `user` – Name of the MongoDB user.
- `password` – Password of the MongoDB user.
- `db` – Name of the database.
- `collection` – Name of the collection.
- `options` -  MongoDB connection string options (optional parameter).

or

``` sql
SOURCE(MONGODB(
    uri 'mongodb://localhost:27017/clickhouse'
    collection 'dictionary_source'
))
```

Setting fields:

- `uri` - URI for establish the connection.
- `collection` – Name of the collection.

[More information about the engine](../../engines/table-engines/integrations/mongodb.md)


#### Redis

Example of settings:

``` xml
<source>
    <redis>
        <host>localhost</host>
        <port>6379</port>
        <storage_type>simple</storage_type>
        <db_index>0</db_index>
    </redis>
</source>
```

or

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

Setting fields:

- `host` – The Redis host.
- `port` – The port on the Redis server.
- `storage_type` – The structure of internal Redis storage using for work with keys. `simple` is for simple sources and for hashed single key sources, `hash_map` is for hashed sources with two keys. Ranged sources and cache sources with complex key are unsupported. May be omitted, default value is `simple`.
- `db_index` – The specific numeric index of Redis logical database. May be omitted, default value is 0.

#### Cassandra

Example of settings:

``` xml
<source>
    <cassandra>
        <host>localhost</host>
        <port>9042</port>
        <user>username</user>
        <password>qwerty123</password>
        <keyspase>database_name</keyspase>
        <column_family>table_name</column_family>
        <allow_filtering>1</allow_filtering>
        <partition_key_prefix>1</partition_key_prefix>
        <consistency>One</consistency>
        <where>"SomeColumn" = 42</where>
        <max_threads>8</max_threads>
        <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
    </cassandra>
</source>
```

Setting fields:

- `host` – The Cassandra host or comma-separated list of hosts.
- `port` – The port on the Cassandra servers. If not specified, default port 9042 is used.
- `user` – Name of the Cassandra user.
- `password` – Password of the Cassandra user.
- `keyspace` – Name of the keyspace (database).
- `column_family` – Name of the column family (table).
- `allow_filtering` – Flag to allow or not potentially expensive conditions on clustering key columns. Default value is 1.
- `partition_key_prefix` – Number of partition key columns in primary key of the Cassandra table. Required for compose key dictionaries. Order of key columns in the dictionary definition must be the same as in Cassandra. Default value is 1 (the first key column is a partition key and other key columns are clustering key).
- `consistency` – Consistency level. Possible values: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. Default value is `One`.
- `where` – Optional selection criteria.
- `max_threads` – The maximum number of threads to use for loading data from multiple partitions in compose key dictionaries.
- `query` – The custom query. Optional parameter.

:::note
The `column_family` or `where` fields cannot be used together with the `query` field. And either one of the `column_family` or `query` fields must be declared.
:::

#### PostgreSQL

Example of settings:

``` xml
<source>
  <postgresql>
      <host>postgresql-hostname</hoat>
      <port>5432</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </postgresql>
</source>
```

or

``` sql
SOURCE(POSTGRESQL(
    port 5432
    host 'postgresql-hostname'
    user 'postgres_user'
    password 'postgres_password'
    db 'db_name'
    table 'table_name'
    replica(host 'example01-1' port 5432 priority 1)
    replica(host 'example01-2' port 5432 priority 2)
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

Setting fields:

- `host` – The host on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).
- `port` – The port on the PostgreSQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`).
- `user` – Name of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).
- `password` – Password of the PostgreSQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`).
- `replica` – Section of replica configurations. There can be multiple sections:
    - `replica/host` – The PostgreSQL host.
    - `replica/port` – The PostgreSQL port.
    - `replica/priority` – The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority.
- `db` – Name of the database.
- `table` – Name of the table.
- `where` – The selection criteria. The syntax for conditions is the same as for `WHERE` clause in PostgreSQL. For example, `id > 10 AND id < 20`. Optional parameter.
- `invalidate_query` – Query for checking the dictionary status. Optional parameter. Read more in the section [Refreshing dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime).
- `query` – The custom query. Optional parameter.

:::note
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
:::

### Null

A special source that can be used to create dummy (empty) dictionaries. Such dictionaries can useful for tests or with setups with separated data and query nodes at nodes with Distributed tables.

``` sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```

## Dictionary Key and Fields

<CloudDetails />

The `structure` clause describes the dictionary key and fields available for queries.

XML description:

``` xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

Attributes are described in the elements:

- `<id>` — Key column
- `<attribute>` — Data column: there can be a multiple number of attributes.

DDL query:

``` sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Attributes are described in the query body:

- `PRIMARY KEY` — Key column
- `AttrName AttrType` — Data column. There can be a multiple number of attributes.

## Key

ClickHouse supports the following types of keys:

- Numeric key. `UInt64`. Defined in the `<id>` tag or using `PRIMARY KEY` keyword.
- Composite key. Set of values of different types. Defined in the tag `<key>` or `PRIMARY KEY` keyword.

An xml structure can contain either `<id>` or `<key>`. DDL-query must contain single `PRIMARY KEY`.

:::note
You must not describe key as an attribute.
:::

### Numeric Key

Type: `UInt64`.

Configuration example:

``` xml
<id>
    <name>Id</name>
</id>
```

Configuration fields:

- `name` – The name of the column with keys.

For DDL-query:

``` sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

- `PRIMARY KEY` – The name of the column with keys.

### Composite Key

The key can be a `tuple` from any types of fields. The [layout](#storing-dictionaries-in-memory) in this case must be `complex_key_hashed` or `complex_key_cache`.

:::tip
A composite key can consist of a single element. This makes it possible to use a string as the key, for instance.
:::

The key structure is set in the element `<key>`. Key fields are specified in the same format as the dictionary [attributes](#dictionary-key-and-fields). Example:

``` xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

or

``` sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

For a query to the `dictGet*` function, a tuple is passed as the key. Example: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

## Attributes

Configuration example:

``` xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

or

``` sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Configuration fields:

| Tag                                                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Required |
|------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`                                               | Column name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Yes      |
| `type`                                               | ClickHouse data type: [UInt8](../../sql-reference/data-types/int-uint.md), [UInt16](../../sql-reference/data-types/int-uint.md), [UInt32](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md), [Int8](../../sql-reference/data-types/int-uint.md), [Int16](../../sql-reference/data-types/int-uint.md), [Int32](../../sql-reference/data-types/int-uint.md), [Int64](../../sql-reference/data-types/int-uint.md), [Float32](../../sql-reference/data-types/float.md), [Float64](../../sql-reference/data-types/float.md), [UUID](../../sql-reference/data-types/uuid.md), [Decimal32](../../sql-reference/data-types/decimal.md), [Decimal64](../../sql-reference/data-types/decimal.md), [Decimal128](../../sql-reference/data-types/decimal.md), [Decimal256](../../sql-reference/data-types/decimal.md),[Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md), [String](../../sql-reference/data-types/string.md), [Array](../../sql-reference/data-types/array.md).<br/>ClickHouse tries to cast value from dictionary to the specified data type. For example, for MySQL, the field might be `TEXT`, `VARCHAR`, or `BLOB` in the MySQL source table, but it can be uploaded as `String` in ClickHouse.<br/>[Nullable](../../sql-reference/data-types/nullable.md) is currently supported for [Flat](#flat), [Hashed](#hashed), [ComplexKeyHashed](#complex_key_hashed), [Direct](#direct), [ComplexKeyDirect](#complex_key_direct), [RangeHashed](#range_hashed), Polygon, [Cache](#cache), [ComplexKeyCache](#complex_key_cache), [SSDCache](#ssd_cache), [SSDComplexKeyCache](#complex_key_ssd_cache) dictionaries. In [IPTrie](#ip_trie) dictionaries `Nullable` types are not supported. | Yes      |
| `null_value`                                         | Default value for a non-existing element.<br/>In the example, it is an empty string. [NULL](../syntax.md#null) value can be used only for the `Nullable` types (see the previous line with types description).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Yes      |
| `expression`                                         | [Expression](../../sql-reference/syntax.md#expressions) that ClickHouse executes on the value.<br/>The expression can be a column name in the remote SQL database. Thus, you can use it to create an alias for the remote column.<br/><br/>Default value: no expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | No       |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | If `true`, the attribute contains the value of a parent key for the current key. See [Hierarchical Dictionaries](#hierarchical-dictionaries).<br/><br/>Default value: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | No       |
| `injective`                                          | Flag that shows whether the `id -> attribute` image is [injective](https://en.wikipedia.org/wiki/Injective_function).<br/>If `true`, ClickHouse can automatically place after the `GROUP BY` clause the requests to dictionaries with injection. Usually it significantly reduces the amount of such requests.<br/><br/>Default value: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | No       |
| `is_object_id`                                       | Flag that shows whether the query is executed for a MongoDB document by `ObjectID`.<br/><br/>Default value: `false`.

## Hierarchical Dictionaries

ClickHouse supports hierarchical dictionaries with a [numeric key](#numeric-key).

Look at the following hierarchical structure:

``` text
0 (Common parent)
│
├── 1 (Russia)
│   │
│   └── 2 (Moscow)
│       │
│       └── 3 (Center)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

This hierarchy can be expressed as the following dictionary table.

| region_id | parent_region | region_name  |
|------------|----------------|---------------|
| 1          | 0              | Russia        |
| 2          | 1              | Moscow        |
| 3          | 2              | Center        |
| 4          | 0              | Great Britain |
| 5          | 4              | London        |

This table contains a column `parent_region` that contains the key of the nearest parent for the element.

ClickHouse supports the hierarchical property for external dictionary attributes. This property allows you to configure the hierarchical dictionary similar to described above.

The [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) function allows you to get the parent chain of an element.

For our example, the structure of dictionary can be the following:

``` xml
<dictionary>
    <structure>
        <id>
            <name>region_id</name>
        </id>

        <attribute>
            <name>parent_region</name>
            <type>UInt64</type>
            <null_value>0</null_value>
            <hierarchical>true</hierarchical>
        </attribute>

        <attribute>
            <name>region_name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

    </structure>
</dictionary>
```

## Polygon dictionaries {#polygon-dictionaries}

Polygon dictionaries allow you to efficiently search for the polygon containing specified points.
For example: defining a city area by geographical coordinates.

Example of a polygon dictionary configuration:

<CloudDetails />

``` xml
<dictionary>
    <structure>
        <key>
            <attribute>
                <name>key</name>
                <type>Array(Array(Array(Array(Float64))))</type>
            </attribute>
        </key>

        <attribute>
            <name>name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

        <attribute>
            <name>value</name>
            <type>UInt64</type>
            <null_value>0</null_value>
        </attribute>
    </structure>

    <layout>
        <polygon>
            <store_polygon_key_column>1</store_polygon_key_column>
        </polygon>
    </layout>

    ...
</dictionary>
```

The corresponding [DDL-query](../../sql-reference/statements/create/dictionary.md#create-dictionary-query):
``` sql
CREATE DICTIONARY polygon_dict_name (
    key Array(Array(Array(Array(Float64)))),
    name String,
    value UInt64
)
PRIMARY KEY key
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
...
```

When configuring the polygon dictionary, the key must have one of two types:

- A simple polygon. It is an array of points.
- MultiPolygon. It is an array of polygons. Each polygon is a two-dimensional array of points. The first element of this array is the outer boundary of the polygon, and subsequent elements specify areas to be excluded from it.

Points can be specified as an array or a tuple of their coordinates. In the current implementation, only two-dimensional points are supported.

The user can upload their own data in all formats supported by ClickHouse.

There are 3 types of [in-memory storage](#storing-dictionaries-in-memory) available:

- `POLYGON_SIMPLE`. This is a naive implementation, where a linear pass through all polygons is made for each query, and membership is checked for each one without using additional indexes.

- `POLYGON_INDEX_EACH`. A separate index is built for each polygon, which allows you to quickly check whether it belongs in most cases (optimized for geographical regions).
Also, a grid is superimposed on the area under consideration, which significantly narrows the number of polygons under consideration.
The grid is created by recursively dividing the cell into 16 equal parts and is configured with two parameters.
The division stops when the recursion depth reaches `MAX_DEPTH` or when the cell crosses no more than `MIN_INTERSECTIONS` polygons.
To respond to the query, there is a corresponding cell, and the index for the polygons stored in it is accessed alternately.

- `POLYGON_INDEX_CELL`. This placement also creates the grid described above. The same options are available. For each sheet cell, an index is built on all pieces of polygons that fall into it, which allows you to quickly respond to a request.

- `POLYGON`. Synonym to `POLYGON_INDEX_CELL`.

Dictionary queries are carried out using standard [functions](../../sql-reference/functions/ext-dict-functions.md) for working with dictionaries.
An important difference is that here the keys will be the points for which you want to find the polygon containing them.

**Example**

Example of working with the dictionary defined above:

``` sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

As a result of executing the last command for each point in the 'points' table, a minimum area polygon containing this point will be found, and the requested attributes will be output.

**Example**

You can read columns from polygon dictionaries via SELECT query, just turn on the `store_polygon_key_column = 1` in the dictionary configuration or corresponding DDL-query.

Query:

``` sql
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = TinyLog;

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

Result:

``` text
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```

## Regular Expression Tree Dictionary {#regexp-tree-dictionary}

Regular expression tree dictionaries are a special type of dictionary which represent the mapping from key to attributes using a tree of regular expressions. There are some use cases, e.g. parsing of [user agent](https://en.wikipedia.org/wiki/User_agent) strings, which can be expressed elegantly with regexp tree dictionaries.

### Use Regular Expression Tree Dictionary in ClickHouse Open-Source

Regular expression tree dictionaries are defined in ClickHouse open-source using the YAMLRegExpTree source which is provided the path to a YAML file containing the regular expression tree.

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

The dictionary source `YAMLRegExpTree` represents the structure of a regexp tree. For example:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

This config consists of a list of regular expression tree nodes. Each node has the following structure:

- **regexp**: the regular expression of the node.
- **attributes**: a list of user-defined dictionary attributes. In this example, there are two attributes: `name` and `version`. The first node defines both attributes. The second node only defines attribute `name`. Attribute `version` is provided by the child nodes of the second node.
  - The value of an attribute may contain **back references**, referring to capture groups of the matched regular expression. In the example, the value of attribute `version` in the first node consists of a back-reference `\1` to capture group `(\d+[\.\d]*)` in the regular expression. Back-reference numbers range from 1 to 9 and are written as `$1` or `\1` (for number 1). The back reference is replaced by the matched capture group during query execution.
- **child nodes**: a list of children of a regexp tree node, each of which has its own attributes and (potentially) children nodes. String matching proceeds in a depth-first fashion. If a string matches a regexp node, the dictionary checks if it also matches the nodes' child nodes. If that is the case, the attributes of the deepest matching node are assigned. Attributes of a child node overwrite equally named attributes of parent nodes. The name of child nodes in YAML files can be arbitrary, e.g. `versions` in above example.

Regexp tree dictionaries only allow access using the functions `dictGet`, `dictGetOrDefault`, and `dictGetAll`.

Example:

```sql
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

Result:

```text
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

In this case, we first match the regular expression `\d+/tclwebkit(?:\d+[\.\d]*)` in the top layer's second node. The dictionary then continues to look into the child nodes and finds that the string also matches `3[12]/tclwebkit`. As a result, the value of attribute `name` is `Android` (defined in the first layer) and the value of attribute `version` is `12` (defined the child node).

With a powerful YAML configure file, we can use a regexp tree dictionaries as a user agent string parser. We support [uap-core](https://github.com/ua-parser/uap-core) and demonstrate how to use it in the functional test [02504_regexp_dictionary_ua_parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)

#### Collecting Attribute Values

Sometimes it is useful to return values from multiple regular expressions that matched, rather than just the value of a leaf node. In these cases, the specialized [`dictGetAll`](../../sql-reference/functions/ext-dict-functions.md#dictgetall) function can be used. If a node has an attribute value of type `T`, `dictGetAll` will return an `Array(T)` containing zero or more values.

By default, the number of matches returned per key is unbounded. A bound can be passed as an optional fourth argument to `dictGetAll`. The array is populated in _topological order_, meaning that child nodes come before parent nodes, and sibling nodes follow the ordering in the source.

Example:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

Result:

```text
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

#### Matching Modes

Pattern matching behavior can be modified with certain dictionary settings:
- `regexp_dict_flag_case_insensitive`: Use case-insensitive matching (defaults to `false`). Can be overridden in individual expressions with `(?i)` and `(?-i)`.
- `regexp_dict_flag_dotall`: Allow '.' to match newline characters (defaults to `false`).

### Use Regular Expression Tree Dictionary in ClickHouse Cloud

Above used `YAMLRegExpTree` source works in ClickHouse Open Source but not in ClickHouse Cloud. To use regexp tree dictionaries in ClickHouse could, first create a regexp tree dictionary from a YAML file locally in ClickHouse Open Source, then dump this dictionary into a CSV file using the `dictionary` table function and the [INTO OUTFILE](../statements/select/into-outfile.md) clause.

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

The content of csv file is:

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

The schema of dumped file is:

- `id UInt64`: the id of the RegexpTree node.
- `parent_id UInt64`: the id of the parent of a node.
- `regexp String`: the regular expression string.
- `keys Array(String)`: the names of user-defined attributes.
- `values Array(String)`: the values of user-defined attributes.

To create the dictionary in ClickHouse Cloud, first create a table `regexp_dictionary_source_table` with below table structure:

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

Then update the local CSV by

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

You can see how to [Insert Local Files](https://clickhouse.com/docs/en/integrations/data-ingestion/insert-local-files) for more details. After we initialize the source table, we can create a RegexpTree by table source:

``` sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```

## Embedded Dictionaries

<SelfManaged />

ClickHouse contains a built-in feature for working with a geobase.

This allows you to:

- Use a region’s ID to get its name in the desired language.
- Use a region’s ID to get the ID of a city, area, federal district, country, or continent.
- Check whether a region is part of another region.
- Get a chain of parent regions.

All the functions support “translocality,” the ability to simultaneously use different perspectives on region ownership. For more information, see the section “Functions for working with web analytics dictionaries”.

The internal dictionaries are disabled in the default package.
To enable them, uncomment the parameters `path_to_regions_hierarchy_file` and `path_to_regions_names_files` in the server configuration file.

The geobase is loaded from text files.

Place the `regions_hierarchy*.txt` files into the `path_to_regions_hierarchy_file` directory. This configuration parameter must contain the path to the `regions_hierarchy.txt` file (the default regional hierarchy), and the other files (`regions_hierarchy_ua.txt`) must be located in the same directory.

Put the `regions_names_*.txt` files in the `path_to_regions_names_files` directory.

You can also create these files yourself. The file format is as follows:

`regions_hierarchy*.txt`: TabSeparated (no header), columns:

- region ID (`UInt32`)
- parent region ID (`UInt32`)
- region type (`UInt8`): 1 - continent, 3 - country, 4 - federal district, 5 - region, 6 - city; other types do not have values
- population (`UInt32`) — optional column

`regions_names_*.txt`: TabSeparated (no header), columns:

- region ID (`UInt32`)
- region name (`String`) — Can’t contain tabs or line feeds, even escaped ones.

A flat array is used for storing in RAM. For this reason, IDs shouldn’t be more than a million.

Dictionaries can be updated without restarting the server. However, the set of available dictionaries is not updated.
For updates, the file modification times are checked. If a file has changed, the dictionary is updated.
The interval to check for changes is configured in the `builtin_dictionaries_reload_interval` parameter.
Dictionary updates (other than loading at first use) do not block queries. During updates, queries use the old versions of dictionaries. If an error occurs during an update, the error is written to the server log, and queries continue using the old version of dictionaries.

We recommend periodically updating the dictionaries with the geobase. During an update, generate new files and write them to a separate location. When everything is ready, rename them to the files used by the server.

There are also functions for working with OS identifiers and search engines, but they shouldn’t be used.
