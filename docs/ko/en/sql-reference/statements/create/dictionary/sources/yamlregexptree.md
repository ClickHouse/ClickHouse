---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'YAMLRegExpTree 딕셔너리 소스'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'YAML 파일을 정규식 트리 딕셔너리의 소스로 구성합니다.'
doc_type: '참고'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

`YAMLRegExpTree` 소스는 로컬 파일 시스템의 YAML 파일에서 정규식 트리를 로드합니다.
이 소스는 [`regexp_tree`](../layouts/regexp-tree.md) 딕셔너리 레이아웃에서만 사용하도록 설계되었으며,
user agent 파싱과 같은 패턴 기반 lookup에 사용할 수 있는 계층적 정규식-속성 매핑을 제공합니다.

:::note
`YAMLRegExpTree` 소스는 ClickHouse Open Source에서만 사용할 수 있습니다.
ClickHouse Cloud에서는 대신 딕셔너리를 CSV로 내보낸 후 [ClickHouse table source](./clickhouse.md)를 통해 로드하십시오.
자세한 내용은 [Using regexp&#95;tree dictionaries in ClickHouse Cloud](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud)를 참조하십시오.
:::

<div id="configuration">
  ## 구성
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

설정 항목:

| 설정     | 설명                                                                             |
| ------ | ------------------------------------------------------------------------------ |
| `PATH` | 정규식 트리가 포함된 YAML 파일의 절대 경로입니다. DDL로 생성한 경우, 해당 파일은 `user_files` 디렉터리에 있어야 합니다. |

<div id="yaml-file-structure">
  ## YAML 파일 구조
</div>

YAML 파일은 정규식 트리 노드의 목록으로 구성됩니다. 각 노드는 속성과 하위 노드를 가질 수 있으며, 함께 계층 구조를 이룹니다.

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

각 노드는 다음 구조를 가집니다:

* **`regexp`**: 이 노드의 정규식입니다.
* **attributes**: 사용자 정의 딕셔너리 속성입니다(예: `name`, `version`). 속성 값에는 정규식의 캡처 그룹을 참조하는 **역참조**가 포함될 수 있으며, `\1` 또는 `$1`(1-9의 숫자)로 작성합니다. 이는 쿼리 시점에 일치한 캡처 그룹으로 대체됩니다.
* **child nodes**: 하위 노드 목록이며, 각 하위 노드는 자체 속성을 가지고 필요에 따라 더 많은 하위 노드를 포함할 수 있습니다. 하위 노드 목록의 이름은 임의로 정할 수 있습니다(예: 위의 `versions`). 문자열 매칭은 깊이 우선으로 진행됩니다. 문자열이 어떤 노드와 일치하면 그 하위 노드도 확인합니다. 가장 깊은 수준에서 일치한 노드의 속성이 precedence를 가지며, 이름이 같은 상위 노드의 속성을 덮어씁니다.

<div id="related-pages">
  ## 관련 페이지
</div>

* [regexp&#95;tree 딕셔너리 레이아웃](../layouts/regexp-tree.md) — 레이아웃 구성, 쿼리 예시, 일치 모드
* [dictGet](/ko/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/ko/sql-reference/functions/ext-dict-functions#dictGetAll) — regexp 트리 딕셔너리 조회 함수