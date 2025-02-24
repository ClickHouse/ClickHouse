---
sidebar_position: 54
sidebar_label: Profile Guided Optimization (PGO)
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

# Profile Guided Optimization

Profile-Guided Optimization (PGO) is a compiler optimization technique where a program is optimized based on the runtime profile.

According to the tests, PGO helps with achieving better performance for ClickHouse. According to the tests, we see improvements up to 15% in QPS on the ClickBench test suite. The more detailed results are available [here](https://pastebin.com/xbue3HMU). The performance benefits depend on your typical workload - you can get better or worse results.

More information about PGO in ClickHouse you can read in the corresponding GitHub [issue](https://github.com/ClickHouse/ClickHouse/issues/44567).

## How to build ClickHouse with PGO?

There are two major kinds of PGO: [Instrumentation](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) and [Sampling](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (also known as AutoFDO). In this guide is described the Instrumentation PGO with ClickHouse.

1. Build ClickHouse in Instrumented mode. In Clang it can be done via passing `-fprofile-generate` option to `CXXFLAGS`.
2. Run instrumented ClickHouse on a sample workload. Here you need to use your usual workload. One of the approaches could be using [ClickBench](https://github.com/ClickHouse/ClickBench) as a sample workload. ClickHouse in the instrumentation mode could work slowly so be ready for that and do not run instrumented ClickHouse in performance-critical environments.
3. Recompile ClickHouse once again with `-fprofile-use` compiler flags and profiles that are collected from the previous step.

A more detailed guide on how to apply PGO is in the Clang [documentation](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization).

If you are going to collect a sample workload directly from a production environment, we recommend trying to use Sampling PGO.
