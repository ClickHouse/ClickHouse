---
description: 'دليل لبناء ClickHouse من المصدر لمعمارية RISC-V 64'
sidebar_label: 'البناء على Linux لمعمارية RISC-V 64'
sidebar_position: 30
slug: /development/build-cross-riscv
title: 'كيفية بناء ClickHouse على Linux لمعمارية RISC-V 64'
doc_type: 'guide'
---

يوفّر ClickHouse دعمًا تجريبيًا لمعمارية RISC-V. ولا يمكن تمكين جميع الميزات.

<div id="build-clickhouse">
  ## بناء ClickHouse
</div>

لإجراء الترجمة المتقاطعة لـ RISC-V على جهاز غير قائم على معمارية RISC-V:

```bash
cd ClickHouse
mkdir build-riscv64
cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

لن يعمل الملف التنفيذي الناتج إلا على Linux ذي معمارية CPU ‏RISC-V 64.