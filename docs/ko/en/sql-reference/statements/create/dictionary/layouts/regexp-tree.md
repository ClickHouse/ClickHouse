---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: '정규식 트리 딕셔너리 레이아웃'
sidebar_label: 'Regexp 트리'
sidebar_position: 12
description: '패턴 기반 조회용 정규식 트리 딕셔너리를 구성합니다.'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## 개요
</div>

`regexp_tree` 딕셔너리를 사용하면 계층적인 정규 표현식 패턴을 기반으로 키를 값에 매핑할 수 있습니다.
이 딕셔너리는 정확한 키 매칭보다 패턴 매칭 조회(예: 정규식 패턴을 매칭해 user agent 문자열과 같은 문자열을 분류하는 작업)에 최적화되어 있습니다.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="ClickHouse regex tree 딕셔너리 소개" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## YAMLRegExpTree 소스로 정규식 트리 딕셔너리 사용하기
</div>

<CloudNotSupportedBadge />

정규식 트리 딕셔너리는 ClickHouse 오픈소스에서 [`YAMLRegExpTree`](../sources/yamlregexptree.md) 소스를 사용해 정의합니다. 이 소스에는 정규식 트리가 포함된 YAML 파일의 경로를 지정합니다.

```sql title="Query"
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

딕셔너리 소스 [`YAMLRegExpTree`](../sources/yamlregexptree.md)는 regexp 트리의 구조를 나타냅니다. 예를 들면 다음과 같습니다:

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

이 구성은 정규식 트리 노드 목록으로 이루어져 있습니다. 각 노드는 다음과 같은 구조를 가집니다.

* **regexp**: 노드의 정규식입니다.
* **attributes**: 사용자 정의 딕셔너리 속성 목록입니다. 이 예시에는 `name`과 `version`이라는 2개의 속성이 있습니다. 첫 번째 노드는 두 속성을 모두 정의합니다. 두 번째 노드는 `name` 속성만 정의합니다. `version` 속성은 두 번째 노드의 자식 노드에서 제공됩니다.
  * 속성 값에는 일치한 정규식의 캡처 그룹을 참조하는 **역참조**가 포함될 수 있습니다. 예시에서 첫 번째 노드의 `version` 속성 값은 정규식의 캡처 그룹 `(\d+[\.\d]*)`를 참조하는 역참조 `\1`로 이루어져 있습니다. 역참조 번호는 1부터 9까지이며, `$1` 또는 `\1`(1번의 경우)로 작성합니다. 쿼리 실행 중에는 역참조가 일치한 캡처 그룹으로 대체됩니다.
* **자식 노드**: regexp 트리 노드의 자식 목록이며, 각 자식은 자체 속성과 (필요한 경우) 자식 노드를 가집니다. 문자열 매칭은 깊이 우선 방식으로 진행됩니다. 문자열이 regexp 노드와 일치하면, 딕셔너리는 해당 문자열이 그 노드의 자식 노드와도 일치하는지 확인합니다. 일치하면 가장 깊은 수준에서 일치한 노드의 속성이 할당됩니다. 자식 노드의 속성은 이름이 같은 부모 노드의 속성을 덮어씁니다. YAML 파일에서 자식 노드의 이름은 임의로 지정할 수 있으며, 위 예시의 `versions`가 그 예입니다.

regexp 트리 딕셔너리는 `dictGet`, `dictGetOrDefault`, `dictGetAll` 함수로만 접근할 수 있습니다. 예시는 다음과 같습니다.

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

이 경우 먼저 최상위 레이어의 두 번째 노드에서 정규식 `\d+/tclwebkit(?:\d+[\.\d]*)`과 일치하는 항목을 찾습니다.
그런 다음 딕셔너리는 자식 노드를 계속 확인하여 해당 문자열이 `3[12]/tclwebkit`과도 일치함을 확인합니다.
그 결과, 속성 `name`의 값은 `Android`(첫 번째 레이어에서 정의)이고 속성 `version`의 값은 `12`(자식 노드에서 정의)입니다.

정교하게 작성된 YAML 설정 파일을 사용하면 regexp 트리 딕셔너리를 user agent 문자열 파서로 사용할 수 있습니다.
ClickHouse는 [uap-core](https://github.com/ua-parser/uap-core)를 지원하며, functional test [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)에서 사용 방법을 확인할 수 있습니다.

<div id="collecting-attribute-values">
  ### 속성 값 수집
</div>

경우에 따라 리프 노드의 값만 반환하는 대신, 일치한 여러 정규식의 값을 반환하는 것이 유용합니다. 이런 경우 특수한 [`dictGetAll`](/ko/sql-reference/functions/ext-dict-functions.md#dictGetAll) 함수를 사용할 수 있습니다. 노드에 `T` 유형의 속성 값이 있으면 `dictGetAll`은 0개 이상의 값을 담은 `Array(T)`를 반환합니다.

기본적으로 키당 반환되는 일치 항목 수에는 제한이 없습니다. 선택적 네 번째 인수로 제한값을 `dictGetAll`에 전달할 수 있습니다. 배열은 *위상 순서(topological order)* 로 채워지며, 이는 하위 노드가 상위 노드보다 앞에 오고 같은 수준의 노드끼리는 소스에 나온 순서를 따른다는 뜻입니다.

예시:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql title="Query"
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

```text title="Response"
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="matching-modes">
  ### 매칭 모드
</div>

패턴 매칭 동작은 특정 딕셔너리 설정을 사용해 변경할 수 있습니다:

* `regexp_dict_flag_case_insensitive`: 대소문자를 구분하지 않는 매칭을 사용합니다(기본값은 `false`). 개별 표현식에서 `(?i)` 및 `(?-i)`로 재정의할 수 있습니다.
* `regexp_dict_flag_dotall`: `.`이 개행 문자와도 매칭되도록 허용합니다(기본값은 `false`).

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## ClickHouse Cloud에서 정규식 트리 딕셔너리 사용
</div>

[`YAMLRegExpTree`](../sources/yamlregexptree.md) 소스는 ClickHouse Open Source에서는 작동하지만 ClickHouse Cloud에서는 작동하지 않습니다.
ClickHouse Cloud에서 regexp 트리 딕셔너리를 사용하려면, 먼저 ClickHouse Open Source에서 YAML 파일로 regexp 트리 딕셔너리를 로컬에 생성한 다음, `dictionary` 테이블 함수와 [INTO OUTFILE](/ko/sql-reference/statements/select/into-outfile.md) 절을 사용해 이 딕셔너리를 CSV 파일로 덤프합니다.

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

CSV 파일 내용은 다음과 같습니다:

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

덤프 파일의 스키마는 다음과 같습니다:

* `id UInt64`: RegexpTree 노드의 id입니다.
* `parent_id UInt64`: 노드의 부모 id입니다.
* `regexp String`: 정규식 문자열입니다.
* `keys Array(String)`: 사용자 정의 속성의 이름입니다.
* `values Array(String)`: 사용자 정의 속성의 값입니다.

ClickHouse Cloud에서 딕셔너리를 생성하려면 먼저 아래 테이블 구조로 `regexp_dictionary_source_table` 테이블을 생성합니다:

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

그런 다음 다음과 같이 로컬 CSV를 업데이트합니다

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

자세한 내용은 [로컬 파일 삽입](/ko/integrations/data-ingestion/insert-local-files)을 참조하십시오. 원본 테이블을 초기화한 후에는 테이블 소스를 사용해 RegexpTree를 생성할 수 있습니다:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```