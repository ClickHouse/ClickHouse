# The memory allocator

A C++ reimplementation of jemalloc (the ClickHouse fork in `contrib/jemalloc`). It exposes the same C ABI
(`je_malloc`, `je_free`, `je_mallocx`, `je_mallctl`, `je_malloc_stats_print`, ... and the public header
`<jemalloc/jemalloc.h>`), so the rest of ClickHouse uses it unchanged. It is enabled with the CMake option
`ENABLE_CPP_ALLOCATOR` in `contrib/jemalloc-cmake`. The options `ENABLE_JEMALLOC_UAF_SAN`,
`JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE` and `JEMALLOC_AARCH64_PAGE_SIZE_KIB` keep their meaning.

The behavior is identical to jemalloc under ClickHouse's configuration: the same size classes, the same decisions
(which slab or extent is chosen, when memory is purged, which arena a thread uses, which allocations are sampled),
the same statistics, `mallctl` results, error codes, messages, heap profile and statistics formats. Known bugs of
jemalloc that affect observable behavior are reproduced deliberately and marked with
`/// jemalloc compatibility:` comments. Every function that corresponds to a jemalloc function says so with a
`/// jemalloc: <name>` comment.

Not implemented: the HPA page allocator, SEC, allocation with `sbrk` (DSS), `prof_log`, user hooks
(`experimental.hooks.install`, `experimental.hooks.remove`), user thread event hooks
(`experimental.hooks.thread_event`, which returns `ENOENT`), custom extent hooks, test hooks. The `mallctl` nodes of these features report
jemalloc's values for disabled features.

## Design

Everything is in `namespace jemalloc`; only the `je_*` symbols are exported. The code does not allocate memory
(other than through itself), does not use exceptions, RTTI or static constructors: all global state is
constant-initialized (`constinit`). Platform differences are `constexpr` constants in `Config.h` selected with
`if constexpr` and policy templates.

From the bottom up:

| Layer | Files | jemalloc |
|---|---|---|
| Configuration and utilities | `Config.h`, `Common.h`, `Format`, `BufferedWriter`, `FixedPoint`, `Nanoseconds`, `PRNG.h`, `Ticker.h`, `Spin.h`, `Mutex`, `Hash.h`, `IntrusiveList.h`, `PairingHeap.h` | preamble, `malloc_io.c`, `buf_writer.c`, `fxp.c`, `nstime.c`, `prng.h`, `ticker.h`, `mutex.c`, `hash.h`, `ql.h`, `ph.h` |
| Size classes | `SizeClassConstants.h`, `SizeClasses`, `Bitmap` | `sc.c`, `sz.c`, `bin_info.c`, `div.c`, `bitmap.c` |
| Extent metadata | `Extent`, `ExtentPool`, `Base`, `RadixTree`, `ExtentMap`, `Pages`, `ExtentHooks` | `edata.c`, `edata_cache.c`, `base.c`, `rtree.c`, `emap.c`, `pages.c`, `ehooks.c` |
| Page allocator | `ExtentSet`, `ExtentCache`, `ExtentOps`, `ExponentialGrow.h`, `Decay`, `PageAllocator`, `Sanitizer` | `eset.c`, `ecache.c`, `extent.c`, `exp_grow.c`, `decay.c`, `pac.c`, `pa.c`, `san.c`, `san_bump.c` |
| Arenas | `Bin`, `Arena`, `ArenaLarge.cpp`, `ArenaInlines.h`, `Arenas` | `bin.c`, `arena.c`, `large.c`, arena selection in `jemalloc.c` |
| Threads | `CacheBin`, `ThreadCache`, `ThreadEvent`, `ThreadState` | `cache_bin.c`, `tcache.c`, `thread_event.c`, `tsd.c` |
| Front-end | `Options`, `MallocConf`, `Init`, `Fork.cpp`, `Frontend.h`, `InternalMalloc.h`, `API.cpp`, `BatchAlloc.cpp`, `BackgroundThread`, `Zone.cpp` | `jemalloc.c`, `conf.c`, `background_thread.c`, `zone.c` |
| Profiling | `Profiling`, `ProfilingData.cpp`, `ProfilingSystem.cpp`, `ProfilingRecent.cpp`, `ProfilingStats.cpp`, `ProfilingHooks.h`, `ProfilingTree.h`, `CuckooHash` | `prof*.c`, `ckh.c` |
| Introspection | `Mallctl*`, `Emitter.h`, `Stats` | `ctl.c`, `emitter.h`, `stats.c` |

## Testing

The tests are in `tests/` and are built by the standalone CMake project:

```
cmake -S base/allocator -B build_alloc -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang \
    -DALLOCATOR_LG_PAGE=16 -DALLOCATOR_DEBUG=1 \
    -DALLOCATOR_REFERENCE_JEMALLOC=$PWD/build/contrib/jemalloc-cmake/lib_jemalloc.a \
    -DALLOCATOR_REFERENCE_INCLUDE=$PWD/build/contrib/jemalloc-cmake/include_linux_aarch64/jemalloc/internal
ninja -C build_alloc && ctest --test-dir build_alloc
```

- Unit tests pin exact numeric behavior of components.
- Oracle tests (`*_oracle.cpp`) run the same randomized operation sequences on a component of the C jemalloc
  (from a ClickHouse build with `ENABLE_CPP_ALLOCATOR=OFF`, which exports its internal functions) and on the C++
  component, and compare the complete state after every step.
- The differential test `tests/diff/run.sh` builds `tests/diff/driver.c`, a deterministic workload against the
  `je_*` API, with both libraries, and requires byte-identical output (every returned address, usable size,
  `mallctl` value, `malloc_stats_print` output and heap profile) under a matrix of `MALLOC_CONF` variants:

  ```
  bash base/allocator/tests/diff/run.sh build/contrib/jemalloc-cmake/lib_jemalloc.a \
      build/contrib/libunwind-cmake/libunwind.a <release build of base/allocator> tmp/diff all
  ```

  Both libraries must be built with the same page size. Use `CPUS=0-3` to boot with per-CPU arenas.
- `tests/diff/bench.c` compares the performance of the two libraries.
