---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u5728\u5185\u5B58\u4E2D\u5B58\u50A8\u5B57\u5178"
---

# 在内存中存储字典 {#dicts-external-dicts-dict-layout}

有多种方法可以将字典存储在内存中。

我们建议 [平](#flat), [散列](#dicts-external_dicts_dict_layout-hashed) 和 [complex_key_hashed](#complex-key-hashed). 其提供最佳的处理速度。

不建议使用缓存，因为性能可能较差，并且难以选择最佳参数。 阅读更多的部分 “[缓存](#cache)”.

有几种方法可以提高字典性能:

-   调用该函数以使用后的字典 `GROUP BY`.
-   将要提取的属性标记为"注射"。 如果不同的属性值对应于不同的键，则称为注射属性。 所以当 `GROUP BY` 使用由键获取属性值的函数，此函数会自动取出 `GROUP BY`.

ClickHouse为字典中的错误生成异常。 错误示例:

-   无法加载正在访问的字典。
-   查询错误 `cached` 字典

您可以查看外部字典的列表及其状态 `system.dictionaries` 桌子

配置如下所示:

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

相应的 [DDL-查询](../../statements/create.md#create-dictionary-query):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## 在内存中存储字典的方法 {#ways-to-store-dictionaries-in-memory}

-   [平](#flat)
-   [散列](#dicts-external_dicts_dict_layout-hashed)
-   [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [缓存](#cache)
-   [直接](#direct)
-   [range_hashed](#range-hashed)
-   [complex_key_hashed](#complex-key-hashed)
-   [complex_key_cache](#complex-key-cache)
-   [ip_trie](#ip-trie)

### 平 {#flat}

字典以平面数组的形式完全存储在内存中。 字典使用多少内存？ 量与最大键的大小（在使用的空间中）成正比。

字典键具有 `UInt64` 类型和值限制为500,000。 如果在创建字典时发现较大的键，ClickHouse将引发异常，不会创建字典。

支持所有类型的来源。 更新时，数据（来自文件或表）将完整读取。

此方法在存储字典的所有可用方法中提供了最佳性能。

配置示例:

``` xml
<layout>
  <flat />
</layout>
```

或

``` sql
LAYOUT(FLAT())
```

### 散列 {#dicts-external_dicts_dict_layout-hashed}

该字典以哈希表的形式完全存储在内存中。 字典中可以包含任意数量的带有任意标识符的元素，在实践中，键的数量可以达到数千万项。

支持所有类型的来源。 更新时，数据（来自文件或表）将完整读取。

配置示例:

``` xml
<layout>
  <hashed />
</layout>
```

或

``` sql
LAYOUT(HASHED())
```

### sparse_hashed {#dicts-external_dicts_dict_layout-sparse_hashed}

类似于 `hashed`，但使用更少的内存，有利于更多的CPU使用率。

配置示例:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

``` sql
LAYOUT(SPARSE_HASHED())
```

### complex_key_hashed {#complex-key-hashed}

这种类型的存储是用于复合 [键](external-dicts-dict-structure.md). 类似于 `hashed`.

配置示例:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range_hashed {#range-hashed}

字典以哈希表的形式存储在内存中，其中包含有序范围及其相应值的数组。

此存储方法的工作方式与散列方式相同，除了键之外，还允许使用日期/时间（任意数字类型）范围。

示例：该表格包含每个广告客户的折扣，格式为:

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

要对日期范围使用示例，请定义 `range_min` 和 `range_max` 中的元素 [结构](external-dicts-dict-structure.md). 这些元素必须包含元素 `name` 和`type` （如果 `type` 如果没有指定，则默认类型将使用-Date）。 `type` 可以是任何数字类型（Date/DateTime/UInt64/Int32/others）。

示例:

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

或

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

要使用这些字典，您需要将附加参数传递给 `dictGetT` 函数，为其选择一个范围:

``` sql
dictGetT('dict_name', 'attr_name', id, date)
```

此函数返回指定的值 `id`s和包含传递日期的日期范围。

算法的详细信息:

-   如果 `id` 未找到或范围未找到 `id`，它返回字典的默认值。
-   如果存在重叠范围，则可以使用任意范围。
-   如果范围分隔符是 `NULL` 或无效日期（如1900-01-01或2039-01-01），范围保持打开状态。 范围可以在两侧打开。

配置示例:

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

或

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

### 缓存 {#cache}

字典存储在具有固定数量的单元格的缓存中。 这些单元格包含经常使用的元素。

搜索字典时，首先搜索缓存。 对于每个数据块，所有在缓存中找不到或过期的密钥都从源请求，使用 `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. 然后将接收到的数据写入高速缓存。

对于缓存字典，过期 [使用寿命](external-dicts-dict-lifetime.md) 可以设置高速缓存中的数据。 如果更多的时间比 `lifetime` 自从在单元格中加载数据以来，单元格的值不被使用，并且在下次需要使用时重新请求它。
这是存储字典的所有方法中最不有效的。 缓存的速度在很大程度上取决于正确的设置和使用场景。 缓存类型字典只有在命中率足够高（推荐99%或更高）时才能表现良好。 您可以查看平均命中率 `system.dictionaries` 桌子

要提高缓存性能，请使用以下子查询 `LIMIT`，并从外部调用字典函数。

支持 [来源](external-dicts-dict-sources.md):MySQL的,ClickHouse的,可执行文件,HTTP.

设置示例:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

或

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

设置足够大的缓存大小。 你需要尝试选择细胞的数量:

1.  设置一些值。
2.  运行查询，直到缓存完全满。
3.  使用评估内存消耗 `system.dictionaries` 桌子
4.  增加或减少单元数，直到达到所需的内存消耗。

!!! warning "警告"
    不要使用ClickHouse作为源，因为处理随机读取的查询速度很慢。

### complex_key_cache {#complex-key-cache}

这种类型的存储是用于复合 [键](external-dicts-dict-structure.md). 类似于 `cache`.

### 直接 {#direct}

字典不存储在内存中，并且在处理请求期间直接转到源。

字典键具有 `UInt64` 类型。

所有类型的 [来源](external-dicts-dict-sources.md)，除了本地文件，支持。

配置示例:

``` xml
<layout>
  <direct />
</layout>
```

或

``` sql
LAYOUT(DIRECT())
```

### ip_trie {#ip-trie}

这种类型的存储用于将网络前缀（IP地址）映射到ASN等元数据。

示例：该表包含网络前缀及其对应的AS号码和国家代码:

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

使用此类布局时，结构必须具有复合键。

示例:

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
        <access_to_key_from_attributes>true</access_to_key_from_attributes>
    </ip_trie>
</layout>
```

或

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

该键必须只有一个包含允许的IP前缀的字符串类型属性。 还不支持其他类型。

对于查询，必须使用相同的函数 (`dictGetT` 与元组）至于具有复合键的字典:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

该函数采用任一 `UInt32` 对于IPv4，或 `FixedString(16)` 碌莽禄Ipv6拢IPv6:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

还不支持其他类型。 该函数返回与此IP地址对应的前缀的属性。 如果有重叠的前缀，则返回最具体的前缀。

数据存储在一个 `trie`. 它必须完全适合RAM。

[原始文章](https://clickhouse.com/docs/en/query_language/dicts/external_dicts_dict_layout/) <!--hide-->
