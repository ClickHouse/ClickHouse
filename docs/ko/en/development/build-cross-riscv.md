---
description: 'RISC-V 64 아키텍처에서 ClickHouse를 from source로 빌드하는 가이드'
sidebar_label: 'RISC-V 64용 Linux 빌드'
sidebar_position: 30
slug: /development/build-cross-riscv
title: 'RISC-V 64용 Linux에서 ClickHouse를 빌드하는 방법'
doc_type: 'guide'
---

ClickHouse는 RISC-V를 Experimental 상태로 지원합니다. 모든 기능을 활성화할 수는 없습니다.

<div id="build-clickhouse">
  ## ClickHouse 빌드
</div>

RISC-V가 아닌 시스템에서 RISC-V용으로 크로스 컴파일하려면:

```bash
cd ClickHouse
mkdir build-riscv64
cmake . -Bbuild-riscv64 -G Ninja -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-riscv64.cmake -DGLIBC_COMPATIBILITY=OFF -DENABLE_LDAP=OFF  -DOPENSSL_NO_ASM=ON -DENABLE_JEMALLOC=ON -DENABLE_PARQUET=OFF -DENABLE_GRPC=OFF -DENABLE_HDFS=OFF -DENABLE_MYSQL=OFF
ninja -C build-riscv64
```

생성된 바이너리는 RISC-V 64 CPU 아키텍처를 사용하는 Linux에서만 실행됩니다.