---
description: 'دليل لبناء ClickHouse من الشيفرة المصدرية لمعمارية AARCH64'
sidebar_label: 'البناء على Linux لمعمارية AARCH64'
sidebar_position: 25
slug: /development/build-cross-arm
title: 'كيفية بناء ClickHouse على Linux لمعمارية AARCH64'
doc_type: 'guide'
---

لا توجد خطوات خاصة مطلوبة لبناء ClickHouse لمعمارية Aarch64 على جهاز يعمل بهذه المعمارية.

لترجمة ClickHouse ترجمة متقاطعة إلى AArch64 على جهاز x86 يعمل بنظام Linux، مرّر الراية التالية إلى `cmake`: `-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake`