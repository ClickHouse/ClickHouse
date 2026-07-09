---
description: '面向 E2K 架构的 ClickHouse 从源码构建指南'
sidebar_label: 'Build on Linux for E2K'
sidebar_position: 35
slug: /development/build-e2k
title: 'Build on Linux for E2K'
doc_type: 'guide'
---

ClickHouse 对 E2K (Elbrus-2000) 提供 Experimental 支持，只能以原生模式编译，所需配置极少，但需要使用 e2k 定制构建的 boost、jemalloc、libunwind、zstd 等库。

<div id="build-clickhouse">
  ## 构建 ClickHouse
</div>

用于构建的 llvm 版本必须不低于 20.1.8。

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

生成的可执行文件只能在 E2K CPU 架构的 Linux 上运行。