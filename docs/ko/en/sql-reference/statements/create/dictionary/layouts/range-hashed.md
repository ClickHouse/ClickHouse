---
slug: /sql-reference/statements/create/dictionary/layouts/range-hashed
title: 'range_hashed 딕셔너리 레이아웃 유형'
sidebar_label: 'range_hashed'
sidebar_position: 5
description: '순서가 있는 날짜/시간 범위를 사용하는 해시 테이블로 딕셔너리를 메모리에 저장합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="range_hashed">
  ## range_hashed
</div>

딕셔너리는 메모리에 해시 테이블 형태로 저장되며, 범위와 그에 대응하는 값의 정렬된 배열을 함께 가집니다.

이 저장 메서드는 hashed와 동일하게 동작하며, 키와 함께 날짜/시간(임의의 숫자 유형) 범위도 추가로 사용할 수 있습니다.

예시: 테이블에는 각 광고주의 할인 정보가 다음 포맷으로 저장됩니다.

```text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

날짜 범위에 샘플을 사용하려면 [구조](../attributes.md#composite-key)에서 `range_min` 및 `range_max` 요소를 정의하십시오. 이 요소들에는 `name` 및 `type` 요소가 있어야 합니다(`type`을 지정하지 않으면 기본 유형인 Date가 사용됩니다). `type`에는 임의의 숫자 유형(Date / DateTime / UInt64 / Int32 / 기타)을 사용할 수 있습니다.

:::note
`range_min` 및 `range_max`의 값은 `Int64` 유형에 맞아야 합니다.
:::

예시:

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

  <TabItem value="xml" label="설정 파일">
    ```xml
    <layout>
        <range_hashed>
            <!-- 겹치는 범위에 대한 전략(min/max). 기본값: min (min(range_min -> range_max) 값을 갖는 일치 범위를 반환) -->
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

이러한 딕셔너리를 사용하려면 범위를 선택할 추가 인수를 `dictGet` 함수에 전달해야 합니다:

```sql
dictGet('dict_name', 'attr_name', id, date)
```

쿼리 예시:

```sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

이 함수는 지정된 `id`에 대해, 전달된 날짜를 포함하는 날짜 범위의 값을 반환합니다.

알고리즘 세부 정보:

* `id`를 찾을 수 없거나 해당 `id`에 대한 범위를 찾을 수 없으면 속성 타입의 기본값을 반환합니다.
* 겹치는 범위가 있고 `range_lookup_strategy=min`이면 `range_min`이 가장 작은 일치 범위를 반환합니다. 여러 범위가 발견되면 `range_max`가 가장 작은 범위를 반환하고, 다시 여러 범위가 발견되면(`range_min`과 `range_max`가 같은 여러 범위가 있는 경우) 그중 임의의 범위를 반환합니다.
* 겹치는 범위가 있고 `range_lookup_strategy=max`이면 `range_min`이 가장 큰 일치 범위를 반환합니다. 여러 범위가 발견되면 `range_max`가 가장 큰 범위를 반환하고, 다시 여러 범위가 발견되면(`range_min`과 `range_max`가 같은 여러 범위가 있는 경우) 그중 임의의 범위를 반환합니다.
* `range_max`가 `NULL`이면 해당 범위는 열린 범위입니다. `NULL`은 가능한 최댓값으로 처리됩니다. `range_min`의 열린 값으로는 `1970-01-01` 또는 `0` (-MAX&#95;INT)을 사용할 수 있습니다.

구성 예시:

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

  <TabItem value="xml" label="설정 파일">
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

겹치는 범위와 열린 범위를 포함한 구성 예시:

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

딕셔너리는 메모리에 범위의 정렬된 배열과 해당 값이 포함된 해시 테이블 형태로 저장됩니다([range&#95;hashed](#range_hashed) 참조). 이 저장 유형은 복합 [키](../attributes.md#composite-key)에 사용됩니다.

구성 예시:

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