---
toc_priority: 41
toc_title: Storing Dictionaries in Memory
---

# Storing Dictionaries in Memory {#dicts-external-dicts-dict-layout}

There are a variety of ways to store dictionaries in memory.

We recommend [flat](#flat), [hashed](#dicts-external_dicts_dict_layout-hashed) and [complex_key_hashed](#complex-key-hashed). which provide optimal processing speed.

Caching is not recommended because of potentially poor performance and difficulties in selecting optimal parameters. Read more in the section “[cache](#cache)”.

There are several ways to improve dictionary performance:

-   Call the function for working with the dictionary after `GROUP BY`.
-   Mark attributes to extract as injective. An attribute is called injective if different attribute values correspond to different keys. So when `GROUP BY` uses a function that fetches an attribute value by the key, this function is automatically taken out of `GROUP BY`.

ClickHouse generates an exception for errors with dictionaries. Examples of errors:

-   The dictionary being accessed could not be loaded.
-   Error querying a `cached` dictionary.

You can view the list of external dictionaries and their statuses in the `system.dictionaries` table.

The configuration looks like this:

``` xml
<yandex>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</yandex>
```

Corresponding [DDL-query](../../../sql-reference/statements/create/dictionary.md):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## Ways to Store Dictionaries in Memory {#ways-to-store-dictionaries-in-memory}

-   [flat](#flat)
-   [hashed](#dicts-external_dicts_dict_layout-hashed)
-   [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [cache](#cache)
-   [ssd_cache](#ssd-cache)
-   [direct](#direct)
-   [range_hashed](#range-hashed)
-   [complex_key_hashed](#complex-key-hashed)
-   [complex_key_cache](#complex-key-cache)
-   [ssd_cache](#ssd-cache)
-   [ssd_complex_key_cache](#complex-key-ssd-cache)
-   [complex_key_direct](#complex-key-direct)
-   [ip_trie](#ip-trie)

### flat {#flat}

The dictionary is completely stored in memory in the form of flat arrays. How much memory does the dictionary use? The amount is proportional to the size of the largest key (in space used).

The dictionary key has the `UInt64` type and the value is limited to 500,000. If a larger key is discovered when creating the dictionary, ClickHouse throws an exception and does not create the dictionary.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

This method provides the best performance among all available methods of storing the dictionary.

Configuration example:

``` xml
<layout>
  <flat />
</layout>
```

or

``` sql
LAYOUT(FLAT())
```

### hashed {#dicts-external_dicts_dict_layout-hashed}

The dictionary is completely stored in memory in the form of a hash table. The dictionary can contain any number of elements with any identifiers In practice, the number of keys can reach tens of millions of items.

The hash table will be preallocated (this will make dictionary load faster), if the is approx number of total rows is known, this is supported only if the source is `clickhouse` without any `<where>` (since in case of `<where>` you can filter out too much rows and the dictionary will allocate too much memory, that will not be used eventually).

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

### sparse_hashed {#dicts-external_dicts_dict_layout-sparse_hashed}

Similar to `hashed`, but uses less memory in favor more CPU usage.

It will be also preallocated so as `hashed`, note that it is even more significant for `sparse_hashed`.

Configuration example:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

``` sql
LAYOUT(SPARSE_HASHED())
```

### complex_key_hashed {#complex-key-hashed}

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `hashed`.

Configuration example:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range_hashed {#range-hashed}

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values.

This storage method works the same way as hashed and allows using date/time (arbitrary numeric type) ranges in addition to the key.

Example: The table contains discounts for each advertiser in the format:

``` text
+---------|-------------|-------------|------+
| advertiser id | discount start date | discount end date | amount |
+===============+=====================+===================+========+
| 123           | 2015-01-01          | 2015-01-15        | 0.15   |
+---------|-------------|-------------|------+
| 123           | 2015-01-16          | 2015-01-31        | 0.25   |
+---------|-------------|-------------|------+
| 456           | 2015-01-01          | 2015-01-15        | 0.05   |
+---------|-------------|-------------|------+
```

To use a sample for date ranges, define the `range_min` and `range_max` elements in the [structure](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). These elements must contain elements `name` and`type` (if `type` is not specified, the default type will be used - Date). `type` can be any numeric type (Date / DateTime / UInt64 / Int32 / others).

Example:

``` xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>last</name>
        <type>Date</type>
    </range_max>
    ...
```

or

``` sql
CREATE DICTIONARY somedict (
    id UInt64,
    first Date,
    last Date
)
PRIMARY KEY id
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
```

To work with these dictionaries, you need to pass an additional argument to the `dictGetT` function, for which a range is selected:

``` sql
dictGetT('dict_name', 'attr_name', id, date)
```

This function returns the value for the specified `id`s and the date range that includes the passed date.

Details of the algorithm:

-   If the `id` is not found or a range is not found for the `id`, it returns the default value for the dictionary.
-   If there are overlapping ranges, you can use any.
-   If the range delimiter is `NULL` or an invalid date (such as 1900-01-01 or 2039-01-01), the range is left open. The range can be open on both sides.

Configuration example:

``` xml
<yandex>
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
</yandex>
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

### cache {#cache}

The dictionary is stored in a cache that has a fixed number of cells. These cells contain frequently used elements.

When searching for a dictionary, the cache is searched first. For each block of data, all keys that are not found in the cache or are outdated are requested from the source using `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. The received data is then written to the cache.

For cache dictionaries, the expiration [lifetime](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) of data in the cache can be set. If more time than `lifetime` has passed since loading the data in a cell, the cell’s value is not used, and it is re-requested the next time it needs to be used.
This is the least effective of all the ways to store dictionaries. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary performs well only when the hit rates are high enough (recommended 99% and higher). You can view the average hit rate in the `system.dictionaries` table.

To improve cache performance, use a subquery with `LIMIT`, and call the function with the dictionary externally.

Supported [sources](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md): MySQL, ClickHouse, executable, HTTP.

Example of settings:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
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

!!! warning "Warning"
    Do not use ClickHouse as a source, because it is slow to process queries with random reads.

### complex_key_cache {#complex-key-cache}

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `cache`.

### ssd_cache {#ssd-cache}

Similar to `cache`, but stores data on SSD and index in RAM.

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
        <path>/var/lib/clickhouse/clickhouse_dictionaries/test_dict</path>
        <!-- Max number on stored keys in the cache. Rounded up to a power of two. -->
        <max_stored_keys>1048576</max_stored_keys>
    </ssd_cache>
</layout>
```

or

``` sql
LAYOUT(CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
    PATH /var/lib/clickhouse/clickhouse_dictionaries/test_dict MAX_STORED_KEYS 1048576))
```

### complex_key_ssd_cache {#complex-key-ssd-cache}

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `ssd_cache`.

### direct {#direct}

The dictionary is not stored in memory and directly goes to the source during the processing of a request.

The dictionary key has the `UInt64` type.

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

### complex_key_direct {#complex-key-direct}

This type of storage is for use with composite [keys](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md). Similar to `direct`.

### ip_trie {#ip-trie}

This type of storage is for mapping network prefixes (IP addresses) to metadata such as ASN.

Example: The table contains network prefixes and their corresponding AS number and country code:

``` text
  +-----------|-----|------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------|-----|------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------|-----|------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------|-----|------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------|-----|------+
```

When using this type of layout, the structure must have a composite key.

Example:

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
```

or

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

The key must have only one String type attribute that contains an allowed IP prefix. Other types are not supported yet.

For queries, you must use the same functions (`dictGetT` with a tuple) as for dictionaries with composite keys:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

The function takes either `UInt32` for IPv4, or `FixedString(16)` for IPv6:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

Other types are not supported yet. The function returns the attribute for the prefix that corresponds to this IP address. If there are overlapping prefixes, the most specific one is returned.

Data is stored in a `trie`. It must completely fit into RAM.

[Original article](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_layout/) <!--hide-->
