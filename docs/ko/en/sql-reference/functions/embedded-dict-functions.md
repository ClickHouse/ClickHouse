---
description: '내장 딕셔너리 작업 함수 문서'
sidebar_label: '내장 딕셔너리'
slug: /sql-reference/functions/ym-dict-functions
title: '내장 딕셔너리 작업 함수'
doc_type: 'reference'
---

:::note
아래 함수가 작동하려면 server 구성에 모든 내장 딕셔너리를 가져오기 위한 경로와 주소가 지정되어 있어야 합니다. 딕셔너리는 이 함수들 중 하나를 처음 호출할 때 로드됩니다. 참조 목록을 로드할 수 없으면 예외가 발생합니다.

따라서 이 섹션에 나오는 예시는 먼저 구성하지 않으면 기본적으로 [ClickHouse Fiddle](https://fiddle.clickhouse.com/)과 quick release 및 프로덕션 배포에서 예외를 발생시킵니다.
:::

참조 목록 생성에 관한 정보는 [&quot;Dictionaries&quot;](../statements/create/dictionary/embedded) 섹션을 참조하십시오.

<div id="multiple-geobases">
  ## Multiple Geobases
</div>

ClickHouse는 특정 지역이 어느 국가에 속하는지에 대한 다양한 관점을 지원하기 위해 여러 대체 지오베이스(지역 계층 구조)를 동시에 사용할 수 있습니다.

`clickhouse-server` 구성은 지역 계층 구조 파일을 지정합니다:

`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

이 파일 외에도 파일명에 `_` 기호와 임의의 접미사가 추가된(파일 확장자 앞) 주변 파일도 검색합니다.
예를 들어 `/opt/geo/regions_hierarchy_ua.txt` 파일이 있으면 이 파일도 찾습니다. 여기서 `ua`를 딕셔너리 키라고 합니다. 접미사가 없는 딕셔너리의 키는 빈 문자열입니다.

모든 딕셔너리는 런타임 중 다시 로드됩니다([`builtin_dictionaries_reload_interval`](/ko/operations/server-configuration-parameters/settings#builtin_dictionaries_reload_interval) 구성 매개변수에 정의된 초 단위 간격마다 1회 또는 기본적으로 1시간마다 1회). 다만 사용 가능한 딕셔너리 목록은 server가 시작될 때 한 번만 결정됩니다.

지역을 처리하는 모든 함수에는 마지막에 선택적 인수가 있으며, 이를 지오베이스라고 합니다.

예시:

```sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToName

region ID와 지오베이스를 받아, 해당 언어의 지역 이름을 문자열로 반환합니다. 지정한 ID의 지역이 없으면 빈 문자열을 반환합니다.

**구문**

```sql
regionToName(id\[, lang\])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* `geobase`에 지정된 해당 언어의 지역 이름입니다. [String](../data-types/string).
* 그렇지 않으면 빈 문자열입니다.

**예시**

```sql title="Query"
SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
```

### regionToCity

지오베이스의 region ID를 입력으로 받습니다. 이 region이 도시이거나 도시의 일부인 경우 해당 도시에 해당하는 region ID를 반환합니다. 그렇지 않으면 0을 반환합니다.

**구문**

```sql
regionToCity(id [, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 해당 도시가 존재하는 경우 해당 도시의 Region ID입니다. [UInt32](../data-types/int-uint).
* 없으면 0입니다.

**예시**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```response title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCity(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                          │
│ World                                      │  0 │                                                          │
│ USA                                        │  0 │                                                          │
│ Colorado                                   │  0 │                                                          │
│ Boulder County                             │  0 │                                                          │
│ Boulder                                    │  5 │ Boulder                                                  │
│ China                                      │  0 │                                                          │
│ Sichuan                                    │  0 │                                                          │
│ Chengdu                                    │  8 │ Chengdu                                                  │
│ America                                    │  0 │                                                          │
│ North America                              │  0 │                                                          │
│ Eurasia                                    │  0 │                                                          │
│ Asia                                       │  0 │                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────┘
```

### regionToArea

region을 영역(지오베이스의 유형 5)으로 변환합니다. 그 밖의 모든 면에서 이 함수는 [&#39;regionToCity&#39;](#regiontocity)와 동일합니다.

**구문**

```sql
regionToArea(id [, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 해당 영역이 존재하면 그 영역의 Region ID를 반환합니다. [UInt32](../data-types/int-uint).
* 없으면 0을 반환합니다.

**예시**

```sql title="Query"
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
```

### regionToDistrict

지역을 연방관구(지오베이스의 유형 4)로 변환합니다. 그 밖의 모든 점에서 이 함수는 &#39;regionToCity&#39;와 동일합니다.

**구문**

```sql
regionToDistrict(id [, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 해당 도시가 존재하면 해당 도시의 Region ID를 반환합니다. [UInt32](../data-types/int-uint).
* 없으면 0을 반환합니다.

**예시**

```sql title="Query"
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

```text title="Response"
┌─regionToName(regionToDistrict(toUInt32(number), \'ua\'))─┐
│                                                          │
│ Central federal district                                 │
│ Northwest federal district                               │
│ South federal district                                   │
│ North Caucases federal district                          │
│ Privolga federal district                                │
│ Ural federal district                                    │
│ Siberian federal district                                │
│ Far East federal district                                │
│ Scotland                                                 │
│ Faroe Islands                                            │
│ Flemish region                                           │
│ Brussels capital region                                  │
│ Wallonia                                                 │
│ Federation of Bosnia and Herzegovina                     │
└──────────────────────────────────────────────────────────┘
```

### regionToCountry

지역을 국가(지오베이스의 유형 3)로 변환합니다. 그 밖의 모든 점에서 이 함수는 &#39;regionToCity&#39;와 동일합니다.

**구문**

```sql
regionToCountry(id [, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 해당 국가에 대응하는 Region ID가 있으면 이를 반환합니다. [UInt32](../data-types/int-uint).
* 없으면 0을 반환합니다.

**예시**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCountry(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                             │
│ World                                      │  0 │                                                             │
│ USA                                        │  2 │ USA                                                         │
│ Colorado                                   │  2 │ USA                                                         │
│ Boulder County                             │  2 │ USA                                                         │
│ Boulder                                    │  2 │ USA                                                         │
│ China                                      │  6 │ China                                                       │
│ Sichuan                                    │  6 │ China                                                       │
│ Chengdu                                    │  6 │ China                                                       │
│ America                                    │  0 │                                                             │
│ North America                              │  0 │                                                             │
│ Eurasia                                    │  0 │                                                             │
│ Asia                                       │  0 │                                                             │
└────────────────────────────────────────────┴────┴─────────────────────────────────────────────────────────────┘
```

### regionToContinent

지역을 대륙(지오베이스의 유형 1)으로 변환합니다. 그 외에는 이 함수는 &#39;regionToCity&#39;와 동일합니다.

**구문**

```sql
regionToContinent(id [, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 해당하는 대륙이 존재하는 경우 그 대륙의 Region ID입니다. [UInt32](../data-types/int-uint).
* 없으면 0입니다.

**예시**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                               │
│ World                                      │  0 │                                                               │
│ USA                                        │ 10 │ North America                                                 │
│ Colorado                                   │ 10 │ North America                                                 │
│ Boulder County                             │ 10 │ North America                                                 │
│ Boulder                                    │ 10 │ North America                                                 │
│ China                                      │ 12 │ Asia                                                          │
│ Sichuan                                    │ 12 │ Asia                                                          │
│ Chengdu                                    │ 12 │ Asia                                                          │
│ America                                    │  9 │ America                                                       │
│ North America                              │ 10 │ North America                                                 │
│ Eurasia                                    │ 11 │ Eurasia                                                       │
│ Asia                                       │ 12 │ Asia                                                          │
└────────────────────────────────────────────┴────┴───────────────────────────────────────────────────────────────┘
```

### regionToTopContinent

해당 지역이 속한 계층 구조에서 최상위 대륙을 찾습니다.

**구문**

```sql
regionToTopContinent(id[, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 최상위 대륙의 식별자입니다(즉, region 계층 구조를 따라 위로 올라갔을 때의 값). [UInt32](../data-types/int-uint).
* 없으면 0입니다.

**예시**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToTopContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                                  │
│ World                                      │  0 │                                                                  │
│ USA                                        │  9 │ America                                                          │
│ Colorado                                   │  9 │ America                                                          │
│ Boulder County                             │  9 │ America                                                          │
│ Boulder                                    │  9 │ America                                                          │
│ China                                      │ 11 │ Eurasia                                                          │
│ Sichuan                                    │ 11 │ Eurasia                                                          │
│ Chengdu                                    │ 11 │ Eurasia                                                          │
│ America                                    │  9 │ America                                                          │
│ North America                              │  9 │ America                                                          │
│ Eurasia                                    │ 11 │ Eurasia                                                          │
│ Asia                                       │ 11 │ Eurasia                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────────────┘
```

### regionToPopulation

지역의 인구를 가져옵니다. 인구 데이터는 지오베이스가 포함된 파일에 기록될 수 있습니다. [&quot;Dictionaries&quot;](../statements/create/dictionary/embedded) 섹션을 참조하십시오. 해당 지역의 인구가 기록되어 있지 않으면 0을 반환합니다. 지오베이스에서는 하위 지역의 인구는 기록되어 있지만 상위 지역의 인구는 기록되어 있지 않을 수 있습니다.

**구문**

```sql
regionToPopulation(id[, geobase])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 지역의 인구입니다. [UInt32](../data-types/int-uint).
* 값이 없으면 0입니다.

**예시**

```sql title="Query"
SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);
```

```text title="Response"
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─population─┐
│                                            │          0 │
│ World                                      │ 4294967295 │
│ USA                                        │  330000000 │
│ Colorado                                   │    5700000 │
│ Boulder County                             │     330000 │
│ Boulder                                    │     100000 │
│ China                                      │ 1500000000 │
│ Sichuan                                    │   83000000 │
│ Chengdu                                    │   20000000 │
│ America                                    │ 1000000000 │
│ North America                              │  600000000 │
│ Eurasia                                    │ 4294967295 │
│ Asia                                       │ 4294967295 │
└────────────────────────────────────────────┴────────────┘
```

### regionIn

`lhs` 지역이 `rhs` 지역에 속하는지 확인합니다. 속하면 1, 속하지 않으면 0인 UInt8 값을 반환합니다.

**구문**

```sql
regionIn(lhs, rhs\[, geobase\])
```

**매개변수**

* `lhs` — 지오베이스의 lhs 지역 ID입니다. [UInt32](../data-types/int-uint).
* `rhs` — 지오베이스의 rhs 지역 ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 속하는 경우 1입니다. [UInt8](../data-types/int-uint).
* 속하지 않는 경우 0입니다.

**구현 세부 사항**

이 관계는 반사적입니다. 즉, 모든 지역은 자기 자신에도 속합니다.

**예시**

```sql title="Query"
SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;
```

```text title="Response"
World is in World
World is not in USA
World is not in Colorado
World is not in Boulder County
World is not in Boulder
USA is in World
USA is in USA
USA is not in Colorado
USA is not in Boulder County
USA is not in Boulder    
```

### regionHierarchy

UInt32 숫자 하나, 즉 지오베이스의 region ID를 받습니다. 전달된 region과 그 상위 region들의 region ID로 이루어진 배열을 반환합니다.

**구문**

```sql
regionHierarchy(id\[, geobase\])
```

**매개변수**

* `id` — 지오베이스의 Region ID입니다. [UInt32](../data-types/int-uint).
* `geobase` — 딕셔너리 키입니다. [Multiple Geobases](#multiple-geobases)를 참조하십시오. [String](../data-types/string). 선택 사항입니다.

**반환 값**

* 전달된 지역과 체인을 따라 있는 모든 상위 지역의 Region ID로 이루어진 [배열](../data-types/array)([UInt32](../data-types/int-uint)).

**예시**

```sql title="Query"
SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);
```

```text title="Response"
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
```

{/* 
  아래 태그의 내부 콘텐츠는 문서 프레임워크를 빌드할 때
  system.functions에서 생성된 문서로 대체됩니다. 태그를 수정하거나 제거하지 마십시오.
  참고: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }