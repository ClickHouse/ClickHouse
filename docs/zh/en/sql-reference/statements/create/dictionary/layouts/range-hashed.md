---
slug: /sql-reference/statements/create/dictionary/layouts/range-hashed
title: 'range_hashed 字典布局类型'
sidebar_label: 'range_hashed'
sidebar_position: 5
description: '使用带有有序日期/时间范围的哈希表将字典存储在内存中。'
doc_type: '参考'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="range_hashed">
  ## range_hashed
</div>

字典以哈希表的形式存储在内存中，其中包含按顺序排列的范围数组及其对应的值。

这种存储方法与 hashed 的工作方式相同，并且除了键之外，还支持使用日期/时间范围 (任意数值类型) 。

示例：该表包含每个广告主的折扣，格式如下：

```text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

如需对日期范围使用样本，请在[结构](../attributes.md#composite-key)中定义 `range_min` 和 `range_max` 元素。这些元素必须包含 `name` 和 `type` 子元素 (如果未指定 `type`，则使用默认类型 Date) 。`type` 可以是任意数值类型 (Date / DateTime / UInt64 / Int32 / others) 。

:::note
`range_min` 和 `range_max` 的值应能容纳在 `Int64` 类型中。
:::

示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
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
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
    <layout>
        <range_hashed>
            <!-- 重叠范围的策略（min/max）。默认值：min（返回 `min(range_min -> range_max)` 值最小的匹配范围） -->
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
  </TabItem>
</Tabs>

<br />

要使用这些字典，需要向 `dictGet` 函数传递一个额外参数，用于指定要选择的范围：

```sql
dictGet('dict_name', 'attr_name', id, date)
```

查询示例：

```sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

此函数返回指定 `id` 对应的值，以及包含传入日期的日期范围。

算法详情：

* 如果未找到 `id`，或者未找到该 `id` 对应的范围，则返回该 attribute 类型的默认值。
* 如果存在重叠范围且 `range_lookup_strategy=min`，则返回 `range_min` 最小的匹配范围；如果找到多个这样的范围，则返回 `range_max` 最小的范围；如果仍然有多个范围 (即多个范围具有相同的 `range_min` 和 `range_max`) ，则从中随机返回一个范围。
* 如果存在重叠范围且 `range_lookup_strategy=max`，则返回 `range_min` 最大的匹配范围；如果找到多个这样的范围，则返回 `range_max` 最大的范围；如果仍然有多个范围 (即多个范围具有相同的 `range_min` 和 `range_max`) ，则从中随机返回一个范围。
* 如果 `range_max` 为 `NULL`，则该范围为开放范围。`NULL` 被视为可能的最大值。对于 `range_min`，可以使用 `1970-01-01` 或 `0` (-MAX&#95;INT) 作为开放值。

配置示例：

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY somedict(
        Abcdef UInt64,
        StartTimeStamp UInt64,
        EndTimeStamp UInt64,
        XXXType String DEFAULT ''
    )
    PRIMARY KEY Abcdef
    RANGE(MIN StartTimeStamp MAX EndTimeStamp)
    ```
  </TabItem>

  <TabItem value="xml" label="配置文件">
    ```xml
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
  </TabItem>
</Tabs>

<br />

包含重叠范围和开放范围的配置示例：

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

<div id="complex_key_range_hashed">
  ## complex_key_range_hashed
</div>

字典以哈希表的形式存储在内存中，其中包含按顺序排列的范围数组及其对应的值 (参见 [range&#95;hashed](#range_hashed)) 。这种存储类型用于复合[键](../attributes.md#composite-key)。

配置示例：

```sql
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