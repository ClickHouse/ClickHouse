---
description: 'Files에 대한 문서'
sidebar_label: 'Files'
slug: /sql-reference/functions/files
title: 'Files'
doc_type: 'reference'
---

<div id="file">
  ## file
</div>

파일을 문자열로 읽어 해당 데이터를 지정된 컬럼에 로드합니다. 파일 내용은 해석되지 않습니다.

자세한 내용은 테이블 함수 [file](../table-functions/file.md)를 참조하십시오.

**구문**

```sql
file(path[, default])
```

**인수**

* `path` — [user&#95;files&#95;path](../../operations/server-configuration-parameters/settings.md#user_files_path)를 기준으로 하는 파일의 상대 경로입니다. 와일드카드 `*`, `**`, `?`, `{abc,def}`, `{N..M}`를 지원하며, 여기서 `N`, `M`은 숫자이고 `'abc'`, `'def'`는 문자열입니다.
* `default` — 파일이 없거나 액세스할 수 없는 경우 반환되는 값입니다. 지원되는 데이터 타입: [String](../data-types/string.md) 및 [NULL](/ko/operations/settings/formats#input_format_null_as_default).

**예시**

파일 a.txt와 b.txt의 데이터를 문자열로 테이블에 삽입하는 예시입니다:

```sql
INSERT INTO table SELECT file('a.txt'), file('b.txt');
```