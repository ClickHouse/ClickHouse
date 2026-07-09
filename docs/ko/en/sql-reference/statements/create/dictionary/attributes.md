---
description: '딕셔너리 키와 속성 구성'
sidebar_label: '속성'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: '딕셔너리 속성'
doc_type: '참고'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

`structure` 절은 쿼리에 사용할 수 있는 딕셔너리 키와 필드를 설명합니다.

XML 설명:

```xml
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

속성은 다음 요소에 설명되어 있습니다:

* `<id>` — 키 컬럼
* `<attribute>` — 데이터 컬럼: 속성은 여러 개일 수 있습니다.

DDL 쿼리:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

속성은 쿼리 본문에 다음과 같이 설명됩니다:

* `PRIMARY KEY` — 키 컬럼
* `AttrName AttrType` — 데이터 컬럼입니다. 속성은 여러 개가 있을 수 있습니다.

<div id="key">
  ## 키
</div>

ClickHouse는 다음과 같은 키 유형을 지원합니다:

* 숫자 키. `UInt64`입니다. `<id>` 태그 또는 `PRIMARY KEY` 키워드로 정의합니다.
* 복합 키. 서로 다른 타입의 값으로 이루어진 집합입니다. `<key>` 태그 또는 `PRIMARY KEY` 키워드로 정의합니다.

XML 구조에는 `<id>` 또는 `<key>` 중 하나만 포함할 수 있습니다. DDL 쿼리에는 `PRIMARY KEY`가 하나만 있어야 합니다.

:::note
키를 속성으로 설명해서는 안 됩니다.
:::

<div id="numeric-key">
  ### 숫자 키
</div>

유형: `UInt64`.

구성 예시:

```xml
<id>
    <name>Id</name>
</id>
```

구성 필드:

* `name` – 키를 포함하는 컬럼의 이름입니다.

DDL 쿼리의 경우:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – 키를 포함하는 컬럼의 이름입니다.

<div id="composite-key">
  ### 복합 키
</div>

키는 임의의 타입의 필드로 구성된 `tuple`이 될 수 있습니다. 이 경우 [레이아웃](./layouts/)은 `complex_key_hashed` 또는 `complex_key_cache`여야 합니다.

:::tip
복합 키는 단일 요소 하나로만 구성될 수도 있습니다. 예를 들어 문자열을 키로 사용할 수 있습니다.
:::

키 구조는 `<key>` 요소에서 설정합니다. 키 필드는 딕셔너리 [속성](#attributes)과 동일한 포맷으로 지정합니다. 예시:

```xml
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

또는

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

`dictGet*` 함수에 대한 쿼리에서는 Tuple이 키로 전달됩니다. 예시: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

복합 키가 단일 속성 하나로만 구성된 경우에는 `tuple`로 감싸지 않고도 키 값을 직접 전달할 수 있습니다. 예를 들어, `dictGetString('dict_name', 'attr_name', 'key')`와 `dictGetString('dict_name', 'attr_name', tuple('key'))`는 모두 올바릅니다.

<div id="attributes">
  ## 속성
</div>

구성 예시:

```xml
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

또는

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

구성 필드:

| 태그                                                 | 설명                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | 필수  |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --- |
| `name`                                             | 컬럼 이름입니다.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | 예   |
| `type`                                             | ClickHouse 데이터 타입: [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md).<br />ClickHouse는 딕셔너리의 값을 지정된 데이터 타입으로 변환(cast)하려고 시도합니다. 예를 들어 MySQL에서는 MySQL 원본 테이블의 필드가 `TEXT`, `VARCHAR` 또는 `BLOB`일 수 있지만, ClickHouse에서는 이를 `String`으로 가져올 수 있습니다.<br />[Nullable](../../../data-types/nullable.md)는 현재 [Flat](./layouts/flat), [Hashed](./layouts/hashed), [ComplexKeyHashed](./layouts/hashed#complex_key_hashed), [Direct](./layouts/direct), [ComplexKeyDirect](./layouts/direct#complex_key_direct), [RangeHashed](./layouts/range-hashed), Polygon, [Cache](./layouts/cache), [ComplexKeyCache](./layouts/cache), [SSDCache](./layouts/ssd-cache), [SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache) 딕셔너리에서 지원됩니다. [IPTrie](./layouts/ip-trie) 딕셔너리에서는 `Nullable` 타입이 지원되지 않습니다. | 예   |
| `null_value`                                       | 존재하지 않는 요소의 기본값입니다.<br />예시에서는 빈 문자열입니다. [NULL](../../../syntax.md#null) 값은 `Nullable` 타입에서만 사용할 수 있습니다(이전 줄의 타입 설명 참조).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | 예   |
| `expression`                                       | ClickHouse가 값에 대해 실행하는 [표현식](../../../syntax.md#expressions)입니다.<br />표현식은 원격 SQL 데이터베이스의 컬럼 이름일 수도 있습니다. 따라서 이를 사용해 원격 컬럼의 별칭을 만들 수 있습니다.<br /><br />기본값: 표현식 없음.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | 아니요 |
| <a name="hierarchical-dict-attr" /> `hierarchical` | `true`이면 이 속성에는 현재 키의 부모 키 값이 포함됩니다. [Hierarchical Dictionaries](./layouts/hierarchical)를 참조하십시오.<br /><br />기본값: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | 아니요 |
| `injective`                                        | `id -> attribute` 사상이 [injective](https://en.wikipedia.org/wiki/Injective_function)인지 여부를 나타내는 플래그입니다.<br />`true`이면 ClickHouse는 단사인 딕셔너리에 대한 요청을 `GROUP BY` 절 뒤로 자동 배치할 수 있습니다. 일반적으로 이렇게 하면 이러한 요청 수가 크게 줄어듭니다.<br /><br />기본값: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | 아니요 |
| `is_object_id`                                     | 쿼리가 `ObjectID`를 사용해 MongoDB 문서에 대해 실행되는지 여부를 나타내는 플래그입니다.<br /><br />기본값: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |     |