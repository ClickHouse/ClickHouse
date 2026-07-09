---
description: 'E2K アーキテクチャ向けにソースから ClickHouse をビルドするためのガイド'
sidebar_label: 'Linux で E2K 向けにビルド'
sidebar_position: 35
slug: /development/build-e2k
title: 'Linux で E2K 向けにビルド'
doc_type: 'guide'
---

ClickHouse は E2K (Elbrus-2000) を実験的にサポートしており、boost、jemalloc、libunwind、zstd などの e2k 向けにカスタムビルドされたライブラリを使用することで、最小限の構成でネイティブモードでのみコンパイルできます。

<div id="build-clickhouse">
  ## ClickHouse をビルドする
</div>

ビルドに必要な llvm のバージョンは、20.1.8 以上である必要があります。

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

生成されたバイナリは、E2K CPUアーキテクチャのLinux上でのみ動作します。