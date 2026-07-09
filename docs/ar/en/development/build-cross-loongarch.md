---
description: 'دليل لبناء ClickHouse من المصدر لمعمارية LoongArch64'
sidebar_label: 'البناء على Linux لـ LoongArch64'
sidebar_position: 35
slug: /development/build-cross-loongarch
title: 'البناء على Linux لـ LoongArch64'
doc_type: 'guide'
---

يوفّر ClickHouse دعمًا تجريبيًا لمعمارية LoongArch64

<div id="build-clickhouse">
  ## بناء ClickHouse
</div>

يجب أن يكون إصدار LLVM اللازم للبناء 21.1.0 أو أحدث.

```bash
cd ClickHouse
mkdir build-loongarch64
cmake . -Bbuild-loongarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-loongarch64.cmake
ninja -C build-loongarch64
```

لن يعمل الملف التنفيذي الناتج إلا على Linux وبمعمارية المعالج LoongArch64.