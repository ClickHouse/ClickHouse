---
description: 'ClickHouse의 String 데이터 타입 문서'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

길이에 제한이 없는 문자열입니다. 값에는 null byte를 포함해 임의의 바이트 집합이 들어갈 수 있습니다.
String 타입은 다른 DBMS에서 사용하는 VARCHAR, BLOB, CLOB 등의 타입을 대체합니다.

테이블을 생성할 때 문자열 필드에 숫자 매개변수(예: `VARCHAR(255)`)를 지정할 수 있지만, ClickHouse는 이를 무시합니다.

별칭:

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## 인코딩
</div>

ClickHouse에는 인코딩 개념이 없습니다. 문자열에는 임의의 바이트 집합이 들어갈 수 있으며, 저장되거나 출력될 때도 그대로 유지됩니다.
텍스트를 저장해야 한다면 UTF-8 인코딩 사용을 권장합니다. 적어도 터미널에서 UTF-8을 사용한다면(권장), 값을 변환하지 않고도 읽고 쓸 수 있습니다.
마찬가지로 문자열을 다루는 일부 함수에는 문자열이 UTF-8로 인코딩된 텍스트를 나타내는 바이트 집합을 포함한다고 가정하고 동작하는 별도 버전이 있습니다.
예를 들어, [length](/ko/sql-reference/functions/array-functions#length) 함수는 문자열 길이를 바이트 단위로 계산하고, [lengthUTF8](../functions/string-functions.md#lengthUTF8) 함수는 값이 UTF-8로 인코딩되었다고 가정하여 문자열 길이를 유니코드 코드 포인트 단위로 계산합니다.