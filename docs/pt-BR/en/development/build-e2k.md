---
description: 'Guia para compilar o ClickHouse a partir do código-fonte para a arquitetura E2K'
sidebar_label: 'Compilar no Linux para E2K'
sidebar_position: 35
slug: /development/build-e2k
title: 'Compilar no Linux para E2K'
doc_type: 'guide'
---

O ClickHouse oferece suporte experimental ao E2K (Elbrus-2000) e só pode ser compilado em modo nativo, com configuração mínima, usando bibliotecas compiladas especificamente para E2K, como boost, jemalloc, libunwind e zstd.

<div id="build-clickhouse">
  ## Compilar o ClickHouse
</div>

A versão do LLVM necessária para compilar deve ser igual ou superior a 20.1.8.

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

O binário resultante será executado apenas em Linux com a arquitetura de CPU E2K.