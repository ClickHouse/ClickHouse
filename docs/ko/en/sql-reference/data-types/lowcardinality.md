---
description: 'String 컬럼용 LowCardinality 최적화 문서'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

다른 데이터 타입의 내부 표현을 딕셔너리 인코딩 형식으로 변경합니다.

<div id="syntax">
  ## 구문
</div>

```sql
LowCardinality(data_type)
```

**매개변수**

* `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), 그리고 [Decimal](../../sql-reference/data-types/decimal.md)을 제외한 숫자입니다. 일부 데이터 타입에서는 `LowCardinality`가 비효율적일 수 있으므로, [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types) 설정 설명을 참조하십시오.

<div id="description">
  ## 설명
</div>

`LowCardinality`는 데이터 저장 방식과 데이터 처리 규칙을 바꾸는 상위 구조입니다. ClickHouse는 `LowCardinality` 컬럼에 [딕셔너리 인코딩](https://en.wikipedia.org/wiki/Dictionary_coder)을 적용합니다. 딕셔너리 인코딩된 데이터로 작업하면 많은 애플리케이션에서 [SELECT](../../sql-reference/statements/select/index.md) 쿼리 성능이 크게 향상됩니다.

`LowCardinality` 데이터 타입 사용 효율은 데이터의 다양성에 따라 달라집니다. 딕셔너리에 서로 다른 값이 10,000개 미만이면 ClickHouse는 대체로 데이터 읽기와 저장에서 더 높은 효율을 보입니다. 딕셔너리에 서로 다른 값이 100,000개를 초과하면 일반 데이터 타입을 사용할 때보다 ClickHouse 성능이 더 떨어질 수 있습니다.

문자열을 다룰 때는 [Enum](../../sql-reference/data-types/enum.md) 대신 `LowCardinality`를 사용하는 것이 좋습니다. `LowCardinality`는 더 유연하게 사용할 수 있으며, 같은 수준 또는 그 이상의 효율을 보이는 경우가 많습니다.

<div id="example">
  ## 예시
</div>

`LowCardinality` 컬럼이 있는 테이블을 생성합니다:

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## 관련 설정 및 함수
</div>

설정:

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/ko/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

함수:

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [스키마와 코덱을 활용한 ClickHouse 최적화](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* 블로그: [ClickHouse에서 시계열 데이터 활용하기](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [String 최적화(러시아어 발표 영상)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [영문 슬라이드](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)