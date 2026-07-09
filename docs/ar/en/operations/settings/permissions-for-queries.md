---
description: 'إعدادات أذونات الاستعلامات.'
sidebar_label: 'أذونات الاستعلامات'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: 'أذونات الاستعلامات'
doc_type: 'reference'
---

يمكن تقسيم الاستعلامات في ClickHouse إلى عدة أنواع:

1. استعلامات قراءة البيانات: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2. استعلامات كتابة البيانات: `INSERT`, `OPTIMIZE`.
3. استعلامات تغيير الإعدادات: `SET`, `USE`.
4. استعلامات [DDL](https://en.wikipedia.org/wiki/Data_definition_language): `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5. `KILL QUERY`.

تُنظِّم الإعدادات التالية أذونات المستخدم وفقًا لنوع الاستعلام:

<div id="readonly">
  ## readonly
</div>

يقيّد أذونات استعلامات قراءة البيانات وكتابة البيانات وتغيير الإعدادات.

عند ضبطه على 1، يسمح بما يلي:

* جميع أنواع استعلامات القراءة (مثل SELECT والاستعلامات المكافئة).
* الاستعلامات التي لا تعدّل سوى سياق الجلسة (مثل USE).

عند ضبطه على 2، يسمح بما سبق بالإضافة إلى:

* SET وCREATE TEMPORARY TABLE

  :::tip
  الاستعلامات مثل EXISTS وDESCRIBE وEXPLAIN وSHOW PROCESSLIST وغيرها تُعدّ مكافئة لـ SELECT، لأنها تنفّذ فقط عمليات SELECT من جداول النظام.
  :::

القيم الممكنة:

* 0 — يُسمح باستعلامات القراءة والكتابة وتغيير الإعدادات.
* 1 — يُسمح فقط باستعلامات قراءة البيانات.
* 2 — يُسمح باستعلامات قراءة البيانات وتغيير الإعدادات.

القيمة الافتراضية: 0

:::note
بعد ضبط `readonly = 1`، لا يمكن للمستخدم تغيير الإعدادين `readonly` و`allow_ddl` في الجلسة الحالية.

عند استخدام الطريقة `GET` في [واجهة HTTP](/ar/interfaces/http)، يتم ضبط `readonly = 1` تلقائيًا. لتعديل البيانات، استخدم الطريقة `POST`.

يؤدي ضبط `readonly = 1` إلى منع المستخدم من تغيير الإعدادات. وهناك طريقة لمنع المستخدم من تغيير إعدادات محددة فقط. كما توجد طريقة للسماح بتغيير إعدادات محددة فقط ضمن قيود `readonly = 1`. لمزيد من التفاصيل، راجع [القيود على الإعدادات](../../operations/settings/constraints-on-settings.md).
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

يسمح باستعلامات [DDL](https://en.wikipedia.org/wiki/Data_definition_language) أو يمنعها.

القيم الممكنة:

* 0 — استعلامات DDL غير مسموح بها.
* 1 — استعلامات DDL مسموح بها.

القيمة الافتراضية: 1

:::note
لا يمكنك تشغيل `SET allow_ddl = 1` إذا كانت قيمة `allow_ddl = 0` في الجلسة الحالية.
:::

:::note KILL QUERY
يمكن تنفيذ `KILL QUERY` مع أي مجموعة من إعدادات readonly و allow&#95;ddl.
:::