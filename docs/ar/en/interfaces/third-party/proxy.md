---
description: 'يصف حلول الخوادم الوكيلة التابعة لجهات خارجية والمتاحة لـ ClickHouse'
sidebar_label: 'الخوادم الوكيلة'
sidebar_position: 29
slug: /interfaces/third-party/proxy
title: 'خوادم وكيلة من تطوير جهات خارجية'
doc_type: 'مرجع'
---

<div id="chproxy">
  ## chproxy
</div>

[chproxy](https://github.com/Vertamedia/chproxy) هو وكيل HTTP وموازن تحميل لقاعدة بيانات ClickHouse.

الميزات:

* التوجيه حسب المستخدم والتخزين المؤقت للاستجابات.
* قيود مرنة.
* تجديد تلقائي لشهادة SSL.

مكتوب بلغة Go.

<div id="kittenhouse">
  ## KittenHouse
</div>

صُمِّم [KittenHouse](https://github.com/VKCOM/kittenhouse) ليعمل كوكيل محلي بين ClickHouse وخادم التطبيق عندما يكون تخزين بيانات `INSERT` مؤقتًا على جانب التطبيق غير ممكن أو غير عملي.

الميزات:

* تخزين البيانات مؤقتًا في الذاكرة وعلى القرص.
* التوجيه على مستوى كل جدول.
* موازنة الأحمال والتحقق من الحالة.

مُنفَّذ بلغة Go.

<div id="clickhouse-bulk">
  ## ClickHouse-Bulk
</div>

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) هو مُجمِّع بسيط لعمليات الإدراج في ClickHouse.

الميزات:

* يجمع الطلبات ويرسلها عند بلوغ العتبة أو على فترات زمنية.
* يدعم عدة خوادم بعيدة.
* المصادقة الأساسية.

مُنفَّذ بلغة Go.