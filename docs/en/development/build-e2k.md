---
description: 'Guide for building ClickHouse from source for the E2K architecture'
sidebar_label: 'Build on Linux for E2K'
sidebar_position: 35
slug: /development/build-e2k
title: 'Build on Linux for E2K'
doc_type: 'guide'
---

# Build on Linux for E2K

ClickHouse has experimental support for E2K (Elbrus-2000), and can only be compiled in native mode with minimal configuration using e2k custom-built libraries such as boost, jemalloc, libunwind, zstd.

## Build ClickHouse {#build-clickhouse}

The llvm version required for building must be greater than or equal to 20.1.8.

```bash
cd ClickHouse
mkdir build-e2k
cmake -DCMAKE_CROSSCOMPILING=OFF -DCOMPILER_CACHE=disabled \
 -DCMAKE_C_COMPILER=/usr/lib/llvm-20/bin/clang -DCMAKE_CXX_COMPILER=/usr/lib/llvm-20/bin/clang++ \
 -DLLD_PATH=/usr/lib/llvm-20/bin/ld.lld \
 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr \
 -DGLIBC_COMPATIBILITY=OFF -DENABLE_LIBRARIES=OFF -DWERROR=OFF \
 -DENABLE_SSL=OFF -DENABLE_OPENSSL_DYNAMIC=ON \
 -DUSE_SIMDJSON=OFF -DENABLE_JEMALLOC=OFF -DENABLE_TESTS=OFF -DUSE_SYSTEM_COMPILER_RT=OFF \
 -DBOOST_USE_UCONTEXT=ON -DENABLE_NURAFT=ON -DENABLE_RAPIDJSON=ON -DUSE_LIBFIU=ON ..
ninja -j8
```

The resulting binary will run only on Linux with the E2K CPU architecture.
