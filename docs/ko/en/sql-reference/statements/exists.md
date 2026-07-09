---
description: 'EXISTS SQL 문 참고 문서'
sidebar_label: 'EXISTS'
sidebar_position: 45
slug: /sql-reference/statements/exists
title: 'EXISTS SQL 문'
doc_type: '참고'
---

```sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY|DATABASE] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

단일 `UInt8` 유형의 컬럼 하나를 반환합니다. 테이블 또는 데이터베이스가 존재하지 않으면 이 컬럼에는 값 `0` 하나가 들어가고, 지정된 데이터베이스에 테이블이 존재하면 `1`이 들어갑니다.