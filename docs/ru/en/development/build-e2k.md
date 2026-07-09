---
description: 'Руководство по сборке ClickHouse из исходного кода для архитектуры E2K'
sidebar_label: 'Сборка на Linux для E2K'
sidebar_position: 35
slug: /development/build-e2k
title: 'Сборка на Linux для E2K'
doc_type: 'guide'
---

ClickHouse имеет экспериментальную поддержку E2K (Elbrus-2000) и может быть скомпилирован только в нативном режиме при минимальной конфигурации с использованием специально собранных для E2K библиотек, таких как boost, jemalloc, libunwind и zstd.

<div id="build-clickhouse">
  ## Сборка ClickHouse
</div>

Для сборки требуется LLVM версии 20.1.8 или выше.

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

Полученный бинарный файл будет работать только на Linux с архитектурой CPU E2K.