---
description: 'FORMAT 절 문서'
sidebar_label: 'FORMAT'
slug: /sql-reference/statements/select/format
title: 'FORMAT 절'
doc_type: 'reference'
---

ClickHouse는 [직렬화 포맷](../../../interfaces/formats.md)을 다양하게 지원하며, 이러한 포맷은 쿼리 결과를 비롯한 여러 용도로 사용할 수 있습니다. `SELECT` 출력 포맷을 선택하는 방법은 여러 가지가 있으며, 그중 하나는 쿼리 끝에 `FORMAT format`을 지정해 결과 데이터를 원하는 특정 포맷으로 받는 것입니다.

특정 포맷은 편의성, 다른 시스템과의 통합, 또는 성능 향상을 위해 사용할 수 있습니다.

<div id="default-format">
  ## 기본 포맷
</div>

`FORMAT` 절이 생략되면 기본 포맷이 사용되며, 이는 설정과 ClickHouse 서버에 액세스하는 데 사용되는 인터페이스에 따라 달라집니다. [HTTP 인터페이스](/ko/interfaces/http)와 배치 모드의 [command-line client](../../../interfaces/client.md)에서는 기본 포맷이 `TabSeparated`입니다. 대화형 모드의 command-line client에서는 기본 포맷이 `PrettyCompact`이며(간결하고 사람이 읽기 쉬운 테이블을 출력합니다).

<div id="implementation-details">
  ## 구현 세부 사항
</div>

command-line client를 사용할 때 데이터는 항상 내부의 효율적인 포맷(`Native`)으로 네트워크를 통해 전송됩니다. 클라이언트는 쿼리의 `FORMAT` 절을 자체적으로 해석해 데이터를 직접 포맷하므로, 네트워크와 서버의 추가 부하를 줄일 수 있습니다.