---
description: 'USE SQL 문 설명서'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'USE SQL 문'
doc_type: '참고'
---

```sql
USE [DATABASE] db
```

세션의 현재 데이터베이스를 설정할 수 있습니다.

현재 데이터베이스는 쿼리에서 테이블 이름 앞에 `.`을 사용해 데이터베이스를 명시적으로 지정하지 않은 경우, 테이블을 찾는 데 사용됩니다.

세션이라는 개념이 없으므로 HTTP protocol을 사용할 때는 이 쿼리를 실행할 수 없습니다.