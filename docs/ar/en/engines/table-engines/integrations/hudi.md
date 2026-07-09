---
description: 'يوفر هذا المحرك تكاملًا للقراءة فقط مع جداول Apache Hudi الموجودة
  في Amazon S3.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'محرك جدول Hudi'
doc_type: 'مرجع'
---

يوفر هذا المحرك تكاملًا للقراءة فقط مع جداول Apache [Hudi](https://hudi.apache.org/) الموجودة في Amazon S3.

<div id="create-table">
  ## إنشاء جدول
</div>

يجب أن يكون جدول Hudi موجودًا مسبقًا في S3، إذ لا يقبل هذا الأمر معاملات DDL لإنشاء جدول جديد.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,] [extra_credentials])
```

**معلمات المحرك**

* `url` — عنوان URL للحاوية مع المسار إلى جدول Hudi موجود.
* `aws_access_key_id`, `aws_secret_access_key` - بيانات اعتماد طويلة الأمد لمستخدم حساب [AWS](https://aws.amazon.com/). يمكنك استخدامها لمصادقة طلباتك. هذه المعلمة اختيارية. إذا لم يتم تحديد بيانات الاعتماد، فستُستخدم من ملف التكوين.
* `extra_credentials` - اختياري. يُستخدم لتمرير `role_arn` من أجل الوصول المستند إلى الأدوار في ClickHouse Cloud. راجع [تأمين S3](/ar/cloud/data-sources/secure-s3) لمعرفة خطوات التكوين.

يمكن تحديد معلمات المحرك باستخدام [المجموعات المسماة](/ar/operations/named-collections.md).

**مثال**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

باستخدام المجموعات المُسمّاة:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123</access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

<div id="see-also">
  ## انظر أيضًا
</div>

* [دالة الجدول hudi](/ar/sql-reference/table-functions/hudi.md)