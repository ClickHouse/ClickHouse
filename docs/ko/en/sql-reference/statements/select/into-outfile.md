---
description: 'INTO OUTFILE 절에 대한 문서'
sidebar_label: 'INTO OUTFILE 절'
slug: /sql-reference/statements/select/into-outfile
title: 'INTO OUTFILE 절'
doc_type: 'reference'
---

`INTO OUTFILE` 절은 `SELECT` 쿼리 결과를 **클라이언트** 측의 파일로 출력합니다.

압축된 파일도 지원됩니다. 압축 유형은 파일 이름의 확장자를 기준으로 감지되며(기본적으로 mode `'auto'` 사용), `COMPRESSION` 절에서 명시적으로 지정할 수도 있습니다. 특정 압축 유형의 압축 수준은 `LEVEL` 절에서 지정할 수 있습니다.

**구문**

```sql
SELECT <expr_list> INTO OUTFILE file_name [AND STDOUT] [APPEND | TRUNCATE] [COMPRESSION type [LEVEL level]]
```

`file_name` 및 `type`은 문자열 리터럴입니다. 지원되는 압축 타입은 다음과 같습니다: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

`level`은 숫자 리터럴입니다. 다음 범위의 양의 정수를 지원합니다: `lz4` 타입은 `1-12`, `zstd` 타입은 `1-22`, 그 외 압축 타입은 `1-9`입니다.

<div id="implementation-details">
  ## 구현 세부 사항
</div>

* 이 기능은 [command-line client](../../../interfaces/client.md)와 [clickhouse-local](../../../operations/utilities/clickhouse-local.md)에서 사용할 수 있습니다. 따라서 [HTTP 인터페이스](/ko/interfaces/http)로 전송한 쿼리는 실패합니다.
* 같은 파일 이름의 파일이 이미 있으면 쿼리가 실패합니다.
* 기본 [출력 형식](../../../interfaces/formats.md)은 `TabSeparated`입니다(command-line client의 batch mode와 동일). 변경하려면 [FORMAT](format.md) 절을 사용하십시오.
* 쿼리에 `AND STDOUT`이 포함되면 파일에 기록된 출력이 표준 출력에도 함께 표시됩니다. 압축과 함께 사용하면 평문이 표준 출력에 표시됩니다.
* 쿼리에 `APPEND`가 포함되면 출력이 기존 파일에 추가됩니다. 압축을 사용하는 경우 `APPEND`는 사용할 수 없습니다.
* 이미 존재하는 파일에 기록할 때는 `APPEND` 또는 `TRUNCATE`를 사용해야 합니다.

**예시**

다음 쿼리를 [command-line client](../../../interfaces/client.md)로 실행하십시오:

```bash title="Query"
clickhouse-client --query="SELECT 1,'ABC' INTO OUTFILE 'select.gz' FORMAT CSV;"
zcat select.gz 
```

```text title="Response"
1,"ABC"
```