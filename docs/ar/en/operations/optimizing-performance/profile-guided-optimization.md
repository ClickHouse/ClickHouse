---
description: 'وثائق حول التحسين الموجّه بالتنميط'
sidebar_label: 'التحسين الموجّه بالتنميط (PGO)'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: 'التحسين الموجّه بالتنميط'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # التحسين الموجّه بالتنميط
</div>

يُعدّ Profile-Guided Optimization ‏(PGO) تقنية تحسين يستخدمها المصرّف، حيث يُحسَّن البرنامج استنادًا إلى التنميط وقت التشغيل.

وفقًا للاختبارات، يساعد PGO في تحقيق أداء أفضل لـ ClickHouse. وتُظهر الاختبارات تحسينات تصل إلى 15% في QPS ضمن مجموعة اختبارات ClickBench. تتوفر النتائج الأكثر تفصيلًا [هنا](https://pastebin.com/xbue3HMU). وتعتمد مكاسب الأداء على عبء العمل المعتاد لديك، لذا قد تحصل على نتائج أفضل أو أسوأ.

يمكنك الاطلاع على مزيد من المعلومات حول PGO في ClickHouse في [issue](https://github.com/ClickHouse/ClickHouse/issues/44567) ذات الصلة على GitHub.

<div id="how-to-build-clickhouse-with-pgo">
  ## كيفية بناء ClickHouse باستخدام PGO؟
</div>

هناك نوعان رئيسيان من PGO: [Instrumentation](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) و[Sampling](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (ويُعرف أيضًا باسم AutoFDO). يشرح هذا الدليل Instrumentation PGO مع ClickHouse.

1. ابنِ ClickHouse في وضع Instrumented. في Clang، يمكن تنفيذ ذلك عبر تمرير الخيار `-fprofile-generate` إلى `CXXFLAGS`.
2. شغِّل ClickHouse المبني بهذا الوضع على عبء عمل نموذجي. هنا تحتاج إلى استخدام عبء العمل المعتاد لديك. ويمكن أن يكون أحد الأساليب استخدام [ClickBench](https://github.com/ClickHouse/ClickBench) كعبء عمل نموذجي. قد يعمل ClickHouse في وضع Instrumentation ببطء، لذا كن مستعدًا لذلك ولا تشغّل ClickHouse المبني بهذا الوضع في البيئات الحساسة للأداء.
3. أعد ترجمة ClickHouse مرة أخرى باستخدام خيارات المصرّف `-fprofile-use` وملفات التعريف التي جُمعت من الخطوة السابقة.

يوجد دليل أكثر تفصيلًا حول كيفية تطبيق PGO في [وثائق](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization) Clang.

إذا كنت تنوي جمع عبء عمل نموذجي مباشرةً من بيئة production، فنوصي بتجربة Sampling PGO.