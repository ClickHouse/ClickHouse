---
alias: []
description: 'RowBinaryWithNamesAndTypesAndDefaults 포맷 문서'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: '참고'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 설명
</div>

[`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) 포맷과 유사하지만, 각 셀 앞에 해당 컬럼의 `DEFAULT` 값을 사용해야 하는지를 나타내는 1바이트가 추가됩니다. 이는 [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md) 포맷과 정확히 같습니다. 이 조합은 스키마 변경이 가능한 `INSERT`를 지원합니다. 데이터를 기록하는 쪽에서는 헤더에서 컬럼을 생략할 수 있으며(이 경우 대상 컬럼의 `DEFAULT`가 적용됨), 전송하는 컬럼에 대해서는 `NULL`과 혼동하지 않고 개별 셀마다 &quot;컬럼의 `DEFAULT` 사용&quot;으로 표시할 수 있습니다.

이 포맷은 입력 전용입니다.

<div id="wire-format">
  ## Wire 형식
</div>

헤더는 [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md)와 동일합니다.

1. 컬럼 수 `N`을 나타내는 `VarUInt`.
2. 컬럼 이름이 들어 있는 길이 접두사 방식의 `String` `N`개.
3. `N`개의 컬럼 타입 — 텍스트 이름 또는 compact binary encoding 중 하나이며, `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format` 설정으로 제어됩니다.

헤더 다음에 각 행은 `N`개의 셀로 구성됩니다. 각 셀은 다음과 같습니다.

* 단일 `UInt8` 마커 바이트.
  * `0x01` — 대상 컬럼의 `DEFAULT` 표현식을 사용합니다. 뒤에 값 바이트는 오지 않습니다.
  * `0x00` — 값이 뒤따르며, 컬럼 타입의 `RowBinary` 직렬화기를 통해 직렬화됩니다. `Nullable(T)`의 경우 값 바이트는 `Nullable` 널 바이트(값이 non-NULL이면 `0`, `NULL`이면 `1`)로 시작하며, non-NULL인 경우 그 뒤에 내부 값이 옵니다.

<div id="defaults-vs-null">
  ## 기본값과 NULL
</div>

셀별 기본 마커와 `Nullable`에 내장된 널 바이트는 서로 독립적입니다. `Nullable(UInt32) DEFAULT 42` 컬럼은 각 행에서 세 가지 방식으로 전송될 수 있습니다:

| 바이트       | 의미                                           |
| --------- | -------------------------------------------- |
| `01`      | `DEFAULT 42`를 사용합니다.                         |
| `00 01`   | 값 경로를 사용하고, `Nullable` 유형을 통해 `NULL`을 나타냅니다. |
| `00 00 …` | 값 경로를 사용하고, 그 뒤에 NULL이 아닌 내부 값이 옵니다.         |

<div id="schema-evolution">
  ## 스키마 진화
</div>

| 사례                         | 동작                                                                                                                          |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| 파일 헤더에 컬럼이 아예 없음           | `defaults_for_omitted_fields` 설정에 따라, `insertDefaultsForNotSeenColumns`를 통해 대상에 기본값이 채워집니다.                                 |
| 헤더에 컬럼이 있고, 셀 마커가 `0x01`   | 각 행에 `insertDefault`가 적용됩니다.                                                                                                |
| 헤더에 컬럼이 있고, 셀 마커가 `0x00`   | 값이 정상적으로 파싱됩니다.                                                                                                             |
| 헤더에 추가 컬럼이 있지만 대상 테이블에는 없음 | `input_format_skip_unknown_fields = 1`이면 별도 알림 없이 무시됩니다(먼저 마커를 읽고 처리하며, `0x01`이면 추가 작업은 없고, `0x00`이면 타입에 맞게 값을 파싱한 뒤 버립니다). |

<div id="example-usage">
  ## 사용 예시
</div>

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

* 헤더에는 `Nullable(UInt32)` 유형의 `x` 컬럼 1개가 있습니다.
* 단일 셀은 마커 `0x01`을 사용하며, 이는 &quot;`DEFAULT 42`를 사용&quot;한다는 뜻입니다.

<div id="format-settings">
  ## 포맷 설정
</div>

<RowBinaryFormatSettings />