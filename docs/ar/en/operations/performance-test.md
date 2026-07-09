---
description: 'دليل لاختبار أداء الأجهزة وإجراء اختبارات قياس الأداء باستخدام ClickHouse'
sidebar_label: 'اختبار الأجهزة'
sidebar_position: 54
slug: /operations/performance-test
title: 'كيفية اختبار أجهزتك باستخدام ClickHouse'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

يمكنك إجراء اختبار أداء بسيط لـ ClickHouse على أي خادم من دون تثبيت حزم ClickHouse.

<div id="automated-run">
  ## التشغيل الآلي
</div>

يمكنك تشغيل اختبار الأداء باستخدام برنامج نصي واحد.

1. نزّل البرنامج النصي.

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. نفّذ البرنامج النصي.

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. انسخ المخرجات وأرسلها إلى feedback@clickhouse.com

جميع النتائج منشورة هنا: https://clickhouse.com/benchmark/hardware/