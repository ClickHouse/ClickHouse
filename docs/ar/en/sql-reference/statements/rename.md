---
description: 'توثيق تعليمة RENAME'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'تعليمة RENAME'
doc_type: 'reference'
---

تعيد تسمية قواعد البيانات أو الجداول أو القواميس. يمكن إعادة تسمية عدة كيانات في استعلام واحد.
لاحظ أن استعلام `RENAME` الذي يتضمن عدة كيانات هو عملية غير ذرية. ولتبديل أسماء الكيانات تبديلًا ذريًا، استخدم تعليمة [EXCHANGE](./exchange.md).

**البنية**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

يعيد تسمية قواعد البيانات.

**البنية**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

يعيد تسمية جدول واحد أو أكثر.

تُعد إعادة تسمية الجداول عملية بسيطة. إذا حددت قاعدة بيانات مختلفة بعد `TO`، فسينقل الجدول إلى قاعدة البيانات تلك. ومع ذلك، يجب أن تكون الأدلة التي تحتوي على قواعد البيانات موجودة على نظام الملفات نفسه. وإلا فستتم إعادة خطأ.
إذا أعدت تسمية عدة جداول في استعلام واحد، فلن تكون العملية ذرية. وقد تُنفَّذ جزئيًا، وقد تتلقى الاستعلامات في الجلسات الأخرى الخطأ `Table ... does not exist ...`.

**البنية**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**مثال**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

ويمكنك استخدام استعلام SQL أبسط:

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

يعيد تسمية قاموس واحد أو عدة قواميس. ويمكن استخدام هذا الاستعلام لنقل القواميس بين قواعد البيانات.

**البنية**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**راجع أيضًا**

* [القواميس](./create/dictionary/overview.md)