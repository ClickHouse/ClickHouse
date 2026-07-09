---
description: 'MOVE 액세스 엔터티 SQL 문에 대한 문서'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'MOVE 액세스 엔터티 SQL 문'
doc_type: '참고'
---

이 SQL 문은 액세스 엔터티를 한 액세스 스토리지에서 다른 액세스 스토리지로 이동할 수 있도록 합니다.

구문:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

현재 ClickHouse에는 5개의 액세스용 스토리지가 있습니다:

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

예시:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```