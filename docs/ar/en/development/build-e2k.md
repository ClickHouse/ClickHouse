---
description: 'دليل لبناء ClickHouse من المصدر لمعمارية E2K'
sidebar_label: 'البناء على Linux لـ E2K'
sidebar_position: 35
slug: /development/build-e2k
title: 'البناء على Linux لـ E2K'
doc_type: 'guide'
---

يوفّر ClickHouse دعمًا تجريبيًا لمعمارية E2K ‏(Elbrus-2000)، ولا يمكن تجميعه إلا في الوضع الأصلي وبحدّ أدنى من الإعدادات، باستخدام مكتبات e2k مخصّصة مثل boost وjemalloc وlibunwind وzstd.

<div id="build-clickhouse">
  ## بناء ClickHouse
</div>

يجب أن يكون إصدار llvm اللازم للبناء أكبر من أو مساويًا لـ 20.1.8.

```bash
cd ClickHouse
mkdir build-e2k
cmake -DCMAKE_CROSSCOMPILING=OFF -DCOMPILER_CACHE=disabled \
 -DCMAKE_C_COMPILER=/usr/lib/llvm-20/bin/clang -DCMAKE_CXX_COMPILER=/usr/lib/llvm-20/bin/clang++ \
 -DLLD_PATH=/usr/lib/llvm-20/bin/ld.lld \
 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr \
 -DGLIBC_COMPATIBILITY=OFF -DENABLE_LIBRARIES=OFF -DWERROR=OFF \
 -DENABLE_SSL=OFF -DENABLE_OPENSSL_DYNAMIC=ON \
 -DUSE_SIMDJSON=OFF -DENABLE_JEMALLOC=OFF -DENABLE_TESTS=OFF \
 -DBOOST_USE_UCONTEXT=ON -DENABLE_NURAFT=ON -DENABLE_RAPIDJSON=ON -DUSE_LIBFIU=ON ..
ninja -j8
```

لن يعمل الملف التنفيذي الناتج إلا على Linux ذي معمارية المعالج E2K.