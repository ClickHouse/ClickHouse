---
description: '`Null` 테이블에 데이터를 쓸 때는 데이터가 무시됩니다. `Null` 테이블에서
  읽을 때는 응답이 비어 있습니다.'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Null table engine'
doc_type: '참고'
---

`Null` 테이블에 데이터를 쓰면 데이터가 무시됩니다.
`Null` 테이블에서 데이터를 읽으면 응답은 비어 있습니다.

`Null` 테이블 엔진은 변환 후 원본 데이터가 더 이상 필요하지 않은 데이터 변환 작업에 유용합니다.
이 목적을 위해 `Null` 테이블에 materialized view를 생성할 수 있습니다.
테이블에 기록된 데이터는 뷰에서 사용되지만, 원본 원시 데이터는 폐기됩니다.