---
description: 'توثيق USER'
sidebar_label: 'USER'
sidebar_position: 45
slug: /sql-reference/statements/alter/user
title: 'ALTER USER'
doc_type: 'reference'
---

يُعدِّل حسابات مستخدمي ClickHouse.

الصيغة:

```sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | RESET AUTHENTICATION METHODS TO NEW | {IDENTIFIED | ADD IDENTIFIED} {[WITH {plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | WITH NO_PASSWORD | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']} | {WITH ssl_certificate CN 'common_name' | SAN 'TYPE:subject_alt_name'} | {WITH ssh_key BY KEY 'public_key' TYPE 'ssh-rsa|...'} | {WITH http SERVER 'server_name' [SCHEME 'Basic']} [VALID UNTIL datetime]
    [, {[{plaintext_password | sha256_password | sha256_hash | ...}] BY {'password' | 'hash'}} | {ldap SERVER 'server_name'} | {...} | ... [,...]]]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [VALID UNTIL datetime]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [SET variable [=value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE|CONST|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

لاستخدام `ALTER USER`، يجب أن تكون لديك [صلاحية ALTER USER](../../../sql-reference/statements/grant.md#access-management).

تُعد `SET variable = value` اسمًا بديلًا لـ `MODIFY SETTING variable = value`: إذ تغيّر إعدادًا واحدًا فقط مع الإبقاء على بقية الإعدادات كما هي. يُفضَّل استخدامها (أو `MODIFY SETTING`) بدلًا من عبارة `SETTINGS` المجرّدة، لأنها تستبدل قائمة الإعدادات بالكامل وتزيل أيضًا جميع ملفات التعريف الموروثة من الملف الأصل.

<div id="grantees-clause">
  ## عبارة GRANTEES
</div>

يحدّد المستخدمين أو الأدوار المسموح لها بتلقّي [الامتيازات](../../../sql-reference/statements/grant.md#privileges) من هذا المستخدم، بشرط أن يكون هذا المستخدم قد مُنح أيضًا جميع صلاحيات الوصول المطلوبة باستخدام [GRANT OPTION](../../../sql-reference/statements/grant.md#granting-privilege-syntax). خيارات عبارة `GRANTEES`:

* `user` — يحدّد مستخدمًا يمكن لهذا المستخدم منح الامتيازات إليه.
* `role` — يحدّد دورًا يمكن لهذا المستخدم منح الامتيازات إليه.
* `ANY` — يمكن لهذا المستخدم منح الامتيازات لأي شخص. وهذا هو الإعداد الافتراضي.
* `NONE` — لا يمكن لهذا المستخدم منح الامتيازات إلى أيّ شخص.

يمكنك استثناء أي مستخدم أو دور باستخدام التعبير `EXCEPT`. على سبيل المثال: `ALTER USER user1 GRANTEES ANY EXCEPT user2`. وهذا يعني أنه إذا مُنحت بعض الامتيازات إلى `user1` باستخدام `GRANT OPTION`، فسيكون قادرًا على منح هذه الامتيازات لأي شخص باستثناء `user2`.

<div id="examples">
  ## أمثلة
</div>

اجعل الأدوار المُسندة هي الأدوار الافتراضية:

```sql
ALTER USER user DEFAULT ROLE role1, role2
```

إذا لم تكن قد أُسنِدت أي أدوار إلى المستخدم مسبقًا، يطرح ClickHouse استثناء.

عيّن جميع الأدوار المُسنَدة لتكون default:

```sql
ALTER USER user DEFAULT ROLE ALL
```

إذا أُسنِد دور إلى مستخدم لاحقًا، فسيصبح الدور الافتراضي تلقائيًا.

عيّن جميع الأدوار المُسنَدة كأدوار افتراضية، باستثناء `role1` و`role2`:

```sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

يسمح للمستخدم صاحب الحساب `john` بمنح امتيازاته للمستخدم صاحب الحساب `jack`:

```sql
ALTER USER john GRANTEES jack;
```

يضيف طرق مصادقة جديدة للمستخدم مع الإبقاء على الأساليب الحالية:

```sql
ALTER USER user1 ADD IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

ملاحظات:

1. قد لا تدعم الإصدارات الأقدم من ClickHouse بنية استخدام عدة طرق المصادقة. لذلك، إذا كان خادم ClickHouse يحتوي على مستخدمين من هذا النوع ثم جرى خفضه إلى إصدار لا يدعم ذلك، فسيصبح هؤلاء المستخدمون غير قابلين للاستخدام وستتعطل بعض العمليات المرتبطة بالمستخدمين. ولتنفيذ الرجوع إلى إصدار أقدم بسلاسة، يجب ضبط جميع المستخدمين بحيث تكون لكل منهم طريقة مصادقة واحدة فقط قبل خفض الإصدار. وبدلاً من ذلك، إذا جرى خفض إصدار الخادم دون اتباع الإجراء الصحيح، فينبغي حذف المستخدمين المتأثرين.
2. لا يمكن أن يتعايش `no_password` مع طرق مصادقة أخرى لأسباب أمنية.
   لذلك، لا يمكن `ADD` طريقة مصادقة `no_password`. سيتسبب الاستعلام أدناه في حدوث خطأ:

```sql
ALTER USER user1 ADD IDENTIFIED WITH no_password
```

إذا كنت تريد إزالة طرق المصادقة لمستخدم والاعتماد على `no_password`، فيجب استخدام صيغة الاستبدال أدناه.

يعيد تعيين طرق المصادقة ويضيف الأساليب المحددة في الاستعلام (وهو تأثير وجود IDENTIFIED في البداية بدون الكلمة المفتاحية ADD):

```sql
ALTER USER user1 IDENTIFIED WITH plaintext_password by '1', bcrypt_password by '2', plaintext_password by '3'
```

أعِد تعيين طرق المصادقة واحتفظ بآخر طريقة تمت إضافتها:

```sql
ALTER USER user1 RESET AUTHENTICATION METHODS TO NEW
```

<div id="valid-until-clause">
  ## عبارة VALID UNTIL
</div>

تتيح لك تحديد تاريخ انتهاء الصلاحية، واختياريًا، وقت انتهاء صلاحية طريقة المصادقة. وتقبل سلسلة نصية كمعلمة. ويُوصى باستخدام التنسيق `YYYY-MM-DD [hh:mm:ss] [timezone]` لقيم التاريخ والوقت. وتساوي هذه المعلمة، افتراضيًا، القيمة `'infinity'`.
لا يمكن تحديد عبارة `VALID UNTIL` إلا مع طريقة مصادقة، باستثناء الحالة التي لا تُحدَّد فيها أي طريقة مصادقة في الاستعلام. في هذه الحالة، ستُطبَّق عبارة `VALID UNTIL` على جميع طرق المصادقة الموجودة.

أمثلة:

* `ALTER USER name1 VALID UNTIL '2025-01-01'`
* `ALTER USER name1 VALID UNTIL '2025-01-01 12:00:00 UTC'`
* `ALTER USER name1 VALID UNTIL 'infinity'`
* `ALTER USER name1 IDENTIFIED WITH plaintext_password BY 'no_expiration', bcrypt_password BY 'expiration_set' VALID UNTIL'2025-01-01''`