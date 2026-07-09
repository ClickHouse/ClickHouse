---
description: '`Dictionary` 엔진은 딕셔너리 데이터를 ClickHouse 테이블 형식으로 표시합니다.'
sidebar_label: '딕셔너리'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: '딕셔너리 테이블 엔진'
doc_type: 'reference'
---

`Dictionary` 엔진은 [딕셔너리](../../../sql-reference/statements/create/dictionary/overview.md) 데이터를 ClickHouse 테이블 형식으로 표시합니다.

<div id="example">
  ## 예시
</div>

예를 들어, 다음과 같은 구성의 `products` 딕셔너리를 살펴보겠습니다:

```xml
<dictionaries>
    <dictionary>
        <name>products</name>
        <source>
            <odbc>
                <table>products</table>
                <connection_string>DSN=some-db-server</connection_string>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>product_id</name>
            </id>
            <attribute>
                <name>title</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</dictionaries>
```

딕셔너리 데이터를 조회합니다:

```sql
SELECT
    name,
    type,
    key,
    attribute.names,
    attribute.types,
    bytes_allocated,
    element_count,
    source
FROM system.dictionaries
WHERE name = 'products'
```

```text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

이 포맷으로 딕셔너리 데이터를 가져오려면 [dictGet*](/ko/sql-reference/functions/ext-dict-functions) 함수를 사용할 수 있습니다.

하지만 원시 데이터를 가져오거나 `JOIN` 작업을 수행해야 할 때는 이 방식이 유용하지 않습니다. 이런 경우에는 딕셔너리 데이터를 테이블로 표시하는 `Dictionary` 엔진을 사용할 수 있습니다.

구문:

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

사용 예시:

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

좋습니다

테이블에 어떤 내용이 들어 있는지 살펴보겠습니다.

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**관련 항목**

* [딕셔너리 함수](/ko/sql-reference/table-functions/dictionary)