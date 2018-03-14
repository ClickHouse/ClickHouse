<a name="dicts-external_dicts_dict_layout"></a>

# Storing dictionaries in memory

There are [many different ways](external_dicts_dict_layout#dicts-external_dicts_dict_layout-manner) to store dictionaries in memory.

We recommend [flat](external_dicts_dict_layout#dicts-external_dicts_dict_layout-flat), [hashed](external_dicts_dict_layout#dicts-external_dicts_dict_layout-hashed), and [complex_key_hashed](external_dicts_dict_layout#dicts-external_dicts_dict_layout-complex_key_hashed). which provide optimal processing speed.

Caching is not recommended because of potentially poor performance and difficulties in selecting optimal parameters. Read more about this in the "[cache](external_dicts_dict_layout#dicts-external_dicts_dict_layout-cache)" section.

There are several ways to improve dictionary performance:

- Call the function for working with the dictionary after `GROUP BY`.
- Mark attributes to extract as injective. An attribute is called injective if different attribute values correspond to different keys. So when `GROUP BY` uses a function that fetches an attribute value by the key, this function is automatically taken out of `GROUP BY`.

ClickHouse generates an exception for errors with dictionaries. Examples of errors:

- The dictionary being accessed could not be loaded.
- Error querying a `cached` dictionary.

You can view the list of external dictionaries and their statuses in the `system.dictionaries` table.

The configuration looks like this:

```xml
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

<a name="dicts-external_dicts_dict_layout-manner"></a>

## Ways to store dictionaries in memory

- [flat](#dicts-external_dicts_dict_layout-flat)
- [hashed](#dicts-external_dicts_dict_layout-hashed)
- [cache](#dicts-external_dicts_dict_layout-cache)
- [range_hashed](#dicts-external_dicts_dict_layout-range_hashed)
- [complex_key_hashed](#dicts-external_dicts_dict_layout-complex_key_hashed)
- [complex_key_cache](#dicts-external_dicts_dict_layout-complex_key_cache)
- [ip_trie](#dicts-external_dicts_dict_layout-ip_trie)

<a name="dicts-external_dicts_dict_layout-flat"></a>

### flat

The dictionary is completely stored in memory in the form of flat arrays. How much memory does the dictionary use? The amount is proportional to the size of the largest key (in space used).

The dictionary key has the ` UInt64` type and the value is limited to 500,000. If a larger key is discovered when creating the dictionary, ClickHouse throws an exception and does not create the dictionary.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

This method provides the best performance among all available methods of storing the dictionary.

Configuration example:

```xml
<layout>
  <flat />
</layout>
```

<a name="dicts-external_dicts_dict_layout-hashed"></a>

### hashed

The dictionary is completely stored in memory in the form of a hash table. The dictionary can contain any number of elements with any identifiers In practice, the number of keys can reach tens of millions of items.

All types of sources are supported. When updating, data (from a file or from a table) is read in its entirety.

Configuration example:

```xml
<layout>
  <hashed />
</layout>
```

<a name="dicts-external_dicts_dict_layout-complex_key_hashed"></a>

### complex_key_hashed

This type of storage is designed for use with compound [keys](external_dicts_dict_structure#dicts-external_dicts_dict_structure). It is similar to hashed.

Configuration example:

```xml
<layout>
  <complex_key_hashed />
</layout>
```

<a name="dicts-external_dicts_dict_layout-range_hashed"></a>

### range_hashed

The dictionary is stored in memory in the form of a hash table with an ordered array of ranges and their corresponding values.

This storage method works the same way as hashed and allows using date/time ranges  in addition to the key, if they appear in the dictionary.

Example: The table contains discounts for each advertiser in the format:

```
  +---------------+---------------------+-------------------+--------+
  | advertiser id | discount start date | discount end date | amount |
  +===============+=====================+===================+========+
  | 123           | 2015-01-01          | 2015-01-15        | 0.15   |
  +---------------+---------------------+-------------------+--------+
  | 123           | 2015-01-16          | 2015-01-31        | 0.25   |
  +---------------+---------------------+-------------------+--------+
  | 456           | 2015-01-01          | 2015-01-15        | 0.05   |
  +---------------+---------------------+-------------------+--------+
```

To use a sample for date ranges, define `range_min` and `range_max` in [structure](external_dicts_dict_structure#dicts-external_dicts_dict_structure).

Example:

```xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
    </range_min>
    <range_max>
        <name>last</name>
    </range_max>
    ...
```

To work with these dictionaries, you need to pass an additional date argument to the `dictGetT` function:

    dictGetT('dict_name', 'attr_name', id, date)

This function returns the value for the specified `id`s and the date range that includes the passed date.

Details of the algorithm:

- If the ` id` is not found or a range is not found for the ` id`, it returns the default value for the dictionary.
- If there are overlapping ranges, you can use any.
- If the range delimiter is `NULL` or an invalid date (such as 1900-01-01 or 2039-01-01), the range is left open. The range can be open on both sides.

Configuration example:

```xml
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
                                <name>StartDate</name>
                        </range_min>
                        <range_max>
                                <name>EndDate</name>
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

<a name="dicts-external_dicts_dict_layout-cache"></a>

### cache

The dictionary is stored in a cache that has a fixed number of cells. These cells contain frequently used elements.

When searching for a dictionary, the cache is searched first. For each block of data, all keys that are not found in the cache or are outdated are requested from the source using ` SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. The received data is then written to the cache.

For cache dictionaries, the expiration (lifetime &lt;dicts-external_dicts_dict_lifetime&gt;) of data in the cache can be set. If more time than `lifetime` has passed since loading the data in a cell, the cell's value is not used, and it is re-requested the next time it needs to be used.

This is the least effective of all the ways to store dictionaries. The speed of the cache depends strongly on correct settings and the usage scenario. A cache type dictionary performs well only when the hit rates are high enough (recommended 99% and higher). You can view the average hit rate in the `system.dictionaries` table.

To improve cache performance, use a subquery with ` LIMIT`, and call the function with the dictionary externally.

Supported [sources](external_dicts_dict_sources#dicts-external_dicts_dict_sources): MySQL, ClickHouse, executable, HTTP.

Example of settings:

```xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
               <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

Set a large enough cache size. You need to experiment to select the number of cells:

1. Set some value.
2. Run queries until the cache is completely full.
3. Assess memory consumption using the `system.dictionaries` table.
4. Increase or decrease the number of cells until the required memory consumption is reached.

<div class="admonition warning">

Do not use ClickHouse as a source, because it is slow to process queries with random reads.

</div>

<a name="dicts-external_dicts_dict_layout-complex_key_cache"></a>

### complex_key_cache

This type of storage is designed for use with compound [keys](external_dicts_dict_structure#dicts-external_dicts_dict_structure). Similar to `cache`.

<a name="dicts-external_dicts_dict_layout-ip_trie"></a>

### ip_trie


The table stores IP prefixes for each key (IP address), which makes it possible to map IP addresses to metadata such as ASN or threat score.

Example: in the table there are prefixes matches to AS number and country:

```
  +-----------------+-------+--------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------------+-------+--------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------------+-------+--------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------------+-------+--------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------------+-------+--------+
```

When using such a layout, the structure should have the "key" element.

Example:

```xml
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

These key must have only one attribute of type String, containing a valid IP prefix. Other types are not yet supported.

For querying, same functions (dictGetT with tuple) as for complex key dictionaries have to be used:

    dictGetT('dict_name', 'attr_name', tuple(ip))

The function accepts either UInt32 for IPv4 address or FixedString(16) for IPv6 address in wire format:

    dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))

No other type is supported. The function returns attribute for a prefix matching the given IP address. If there are overlapping prefixes, the most specific one is returned.

The data is stored currently in a bitwise trie, it has to fit in memory.
