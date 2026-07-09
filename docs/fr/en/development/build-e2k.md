---
description: 'Guide pour compiler ClickHouse depuis le code source pour l''architecture E2K'
sidebar_label: 'Compiler sous Linux pour E2K'
sidebar_position: 35
slug: /development/build-e2k
title: 'Compiler sous Linux pour E2K'
doc_type: 'guide'
---

ClickHouse offre une prise en charge expérimentale d’E2K (Elbrus-2000) et ne peut être compilé qu’en mode natif, avec une configuration minimale, à l’aide de bibliothèques E2K compilées sur mesure comme boost, jemalloc, libunwind et zstd.

<div id="build-clickhouse">
  ## Compilation de ClickHouse
</div>

La version de LLVM requise pour la compilation doit être supérieure ou égale à 20.1.8.

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

Le binaire obtenu ne fonctionnera que sous Linux sur une architecture CPU E2K.