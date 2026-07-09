---
description: '关于基于性能分析的优化的文档'
sidebar_label: '基于性能分析的优化（PGO）'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: '基于性能分析的优化'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # 基于性能分析的优化
</div>

基于性能分析的优化 (PGO) 是一种编译器优化技术，即根据程序的运行时性能分析结果对其进行优化。

测试表明，PGO 有助于提升 ClickHouse 的性能。根据测试结果，在 ClickBench 测试套件上，QPS 最高可提升 15%。更详细的结果见[这里](https://pastebin.com/xbue3HMU)。性能收益取决于你的典型工作负载，实际效果可能更好，也可能更差。

有关 ClickHouse 中 PGO 的更多信息，请参阅相应的 GitHub [issue](https://github.com/ClickHouse/ClickHouse/issues/44567)。

<div id="how-to-build-clickhouse-with-pgo">
  ## 如何使用 PGO 构建 ClickHouse？
</div>

PGO 主要分为两种：[Instrumentation](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) 和 [Sampling](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (也称为 AutoFDO) 。本指南介绍的是 ClickHouse 的 Instrumentation PGO。

1. 以 Instrumented 模式构建 ClickHouse。在 Clang 中，可以通过向 `CXXFLAGS` 传入 `-fprofile-generate` 选项来实现。
2. 在样本工作负载上运行已插桩的 ClickHouse。这里需要使用你平时的工作负载。一种做法是使用 [ClickBench](https://github.com/ClickHouse/ClickBench) 作为样本工作负载。处于 instrumentation 模式的 ClickHouse 可能运行较慢，因此请提前做好准备，并且不要在对性能敏感的环境中运行已插桩的 ClickHouse。
3. 使用 `-fprofile-use` 编译器标志以及上一步收集到的 profiles，再次编译 ClickHouse。

有关如何应用 PGO 的更详细说明，请参阅 Clang 的[文档](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization)。

如果你打算直接从 production 环境收集样本工作负载，我们建议尝试使用 Sampling PGO。