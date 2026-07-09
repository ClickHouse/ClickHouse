---
description: '프로파일 기반 최적화 문서'
sidebar_label: '프로파일 기반 최적화 (PGO)'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: '프로파일 기반 최적화'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # 프로파일 기반 최적화
</div>

프로파일 기반 최적화(PGO)는 프로그램의 런타임 프로파일을 바탕으로 최적화하는 컴파일러 최적화 기법입니다.

테스트에 따르면 PGO는 ClickHouse의 성능 향상에 도움이 됩니다. 테스트 결과, ClickBench test suite에서 QPS가 최대 15%까지 향상되는 것으로 확인되었습니다. 더 자세한 결과는 [여기](https://pastebin.com/xbue3HMU)에서 확인할 수 있습니다. 성능 향상 효과는 일반적인 작업 부하에 따라 달라질 수 있으므로, 더 좋은 결과가 나올 수도 있고 그렇지 않을 수도 있습니다.

ClickHouse의 PGO에 대한 자세한 내용은 관련 GitHub [issue](https://github.com/ClickHouse/ClickHouse/issues/44567)에서 확인할 수 있습니다.

<div id="how-to-build-clickhouse-with-pgo">
  ## PGO로 ClickHouse를 빌드하는 방법
</div>

PGO에는 두 가지 주요 방식이 있습니다. [Instrumentation](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers)과 [샘플링](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers)(AutoFDO라고도 함)입니다. 이 가이드에서는 ClickHouse의 Instrumentation PGO를 설명합니다.

1. ClickHouse를 Instrumented 모드로 빌드합니다. Clang에서는 `CXXFLAGS`에 `-fprofile-generate` 옵션을 전달해 이를 수행할 수 있습니다.
2. instrumented ClickHouse를 샘플 워크로드에서 실행합니다. 여기서는 평소 사용하는 워크로드를 사용해야 합니다. 한 가지 방법으로 [ClickBench](https://github.com/ClickHouse/ClickBench)를 샘플 워크로드로 사용할 수 있습니다. instrumentation 모드의 ClickHouse는 느리게 동작할 수 있으므로 이를 감안해야 하며, 성능이 중요한 환경에서는 instrumented ClickHouse를 실행하지 마십시오.
3. 이전 단계에서 수집한 프로파일과 `-fprofile-use` 컴파일러 플래그를 사용해 ClickHouse를 다시 컴파일합니다.

PGO 적용 방법에 대한 더 자세한 안내는 Clang [documentation](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)에서 확인할 수 있습니다.

프로덕션 환경에서 직접 샘플 워크로드를 수집하려는 경우에는 샘플링 PGO 사용을 권장합니다.