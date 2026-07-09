---
description: 'توثيق لتعليمة EXECUTE AS'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'تعليمة EXECUTE AS'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # تعليمة EXECUTE AS
</div>

تسمح بتنفيذ الاستعلامات نيابةً عن مستخدم آخر.

<div id="syntax">
  ## الصيغة
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

الصيغة الأولى (من دون `subquery`) تجعل جميع الاستعلامات التالية في الجلسة الحالية تُنفَّذ نيابةً عن `target_user` المحدد.

الصيغة الثانية (مع `subquery`) تنفّذ فقط `subquery` المحدد نيابةً عن `target_user` المحدد.

ولكي تعمل كلتا الصيغتين، يجب ضبط إعداد config ‏`access_control_improvements.allow_impersonate_user`
على القيمة `1`، وأن يكون امتياز `IMPERSONATE` ممنوحًا. على سبيل المثال، الأوامر التالية

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

اسمح للمستخدم `user2` بتنفيذ الأوامر `EXECUTE AS user1 ...`، واسمح أيضًا للمستخدم `user3` بتنفيذ الأوامر نيابةً عن أي مستخدم.

أثناء انتحال هوية مستخدم آخر، تُرجع الدالة [currentUser()](/ar/sql-reference/functions/other-functions#currentUser) اسم ذلك المستخدم الآخر،
وتُرجع الدالة [authenticatedUser()](/ar/sql-reference/functions/other-functions#authenticatedUser) اسم المستخدم الذي تم التحقق من هويته فعليًا.

<div id="examples">
  ## أمثلة
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```