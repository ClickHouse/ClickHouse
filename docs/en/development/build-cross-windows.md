---
description: 'Guide for cross-compiling ClickHouse from Linux for Windows systems'
sidebar_label: 'Build on Linux for Windows'
sidebar_position: 25
slug: /development/build-cross-windows
title: 'Build on Linux for Windows'
doc_type: 'guide'
---

This is for the case when you have a Linux machine and want to use it to build ClickHouse
binaries that run on Windows. There is no native Windows build - the Windows binaries are
always cross-compiled, as they are for macOS and FreeBSD.

:::note
The Windows port is **not finished**. `clickhouse-client` and `clickhouse-local` do not link
yet. What does build is the `clickhouse-windows-ported` target, described in
[Current state](#current-state) below, which CI builds on every pull request so that the
cross-build cannot regress while the remaining work lands. If you are picking this up, start
from [Remaining work](#remaining-work).
:::

The cross-build for Windows is based on the [Build instructions](../development/build.md),
follow them first.

## How it works {#how-it-works}

The target triple is `x86_64-w64-windows-gnu`, that is, mingw-w64 rather than MSVC:

- The output is an ordinary native PE executable. There is no emulation layer of the Cygwin
  kind involved, and no extra runtime DLL to ship: the binaries call the Windows API
  directly.
- Only the Win32 API headers and the CRT import libraries come from mingw-w64. The compiler,
  the linker and the whole C++ runtime are ours - Clang, LLD, and the libc++, libc++abi,
  LLVM libunwind and compiler-rt in `contrib/llvm-project`, exactly as on every other
  platform.
- mingw-w64 is packaged by every Linux distribution and is freely redistributable, so unlike
  an MSVC-targeted build this needs nothing extracted from a Windows installation and no
  Microsoft licence.

Two details are worth knowing when reading the code:

- **Windows is LLP64.** `long` is 32 bits wide, unlike on every other platform ClickHouse
  supports, where it is 64. Code that assumes `sizeof(long) == 8` - `__builtin_clzl` on a
  `size_t`, an `L`-suffixed constant that needs 64 bits, a `long` typedef for a pointer -
  is silently wrong. Several such bugs were found while getting this far.
- **Thread stacks come from the PE header.** Windows has no `RLIMIT_STACK` equivalent, and
  its default of 1 MiB is too small for our recursive-descent parser, so the link sets an
  8 MiB reserve. That covers every thread, not just the main one, which is why - unlike on
  macOS, see `src/Common/ThreadStackSize.h` - no per-thread override is needed.

## Install the cross-compilation toolset {#install-cross-compilation-toolset}

```bash
sudo apt-get install mingw-w64
```

This installs the headers and import libraries under `/usr/x86_64-w64-mingw32`, which is
where the toolchain file looks for them. Point `-DMINGW_SYSROOT=...` at a different sysroot
if yours lives elsewhere. Nothing else from the package is used - in particular not its GCC.

## Build ClickHouse {#build-clickhouse}

```bash
cd ClickHouse
cmake -S . -B build-windows -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=$PWD/cmake/windows/toolchain-x86_64.cmake \
    -DENABLE_LIBRARIES=OFF \
    -DENABLE_TESTS=OFF \
    -DENABLE_UTILS=OFF \
    -DENABLE_CLICKHOUSE_ALL=OFF
ninja -C build-windows clickhouse-windows-ported
```

`ENABLE_LIBRARIES=OFF` is what keeps the optional third-party libraries - Kafka, HDFS, the
cloud storage SDKs and so on - out of the build. None of them has been looked at for
Windows, and the client and local modes do not need them.

The resulting binaries are in the PE format and cannot be run on Linux. To test them without
a Windows machine, use Wine - but note that an `x86_64` PE binary needs an `x86_64` host, or
an emulator such as FEX on ARM:

```bash
wine build-windows/programs/clickhouse.exe local --query "SELECT 1"
```

## Current state {#current-state}

What compiles for Windows today:

- the C++ runtime: libc++, libc++abi, LLVM libunwind (driving SEH on `x86_64`) and the
  compiler-rt builtins;
- 117 third-party libraries, among them OpenSSL, Boost, zlib-ng, zstd, LZ4, re2, Abseil,
  cctz, c-ares, replxx and Snappy;
- all of Poco - Foundation, Net, XML, JSON, Util and NetSSL.

`clickhouse-windows-ported`, defined in `cmake/windows/ported_targets.cmake`, is the first two
of those - the third cannot be a cmake target yet, see
[the gap in the CI coverage](#ci-coverage-gap).

The target is derived from the contrib list rather than being a hand-written copy of it, so a
newly added third-party library is covered automatically. If yours does not build for Windows
and nothing in the client or local modes needs it, add it to `WINDOWS_UNPORTED_TARGETS` in
that file with a note - do not silently drop the coverage.

Not built for Windows, and why:

| Component | Reason |
|---|---|
| `libuv` | Needs its `src/win/` sources selected. Only reached through nats-io, Cassandra and AMQP-CPP. |
| `libxml2` | Wants `iconv.h`, and its `off_t` use assumes LP64. Only reached through libhdfs3 and the Azure SDK. |
| `libarchive` | Its `config.h` in `contrib/libarchive-cmake` describes a POSIX host. Until one exists for Windows, `clickhouse-local` there cannot read archives. |
| `jemalloc` | Builds, but its `pages_map` cannot honour our `MADV_DONTNEED`-based purging. A client does not need a custom allocator. |
| Keeper, the JIT, the DWARF parser, `libfiu`, `liburing`, `numactl`, Rust | Server-side, Linux-specific, or no Windows target in the CI image. See `cmake/target.cmake`. |
| `Poco::Net::NetworkInterface`, `MulticastSocket` | Upstream's Windows implementation needs members our Poco fork removed along with the rest of its Windows support. Nothing in ClickHouse uses either. |

### A gap in the CI coverage {#ci-coverage-gap}

Poco compiles for Windows but is **not** part of `clickhouse-windows-ported`, and so is not
covered by CI yet. `_poco_foundation` links `clickhouse_common_io` - our fork routes
`Poco::Logger` through ClickHouse's logging - so CMake will not archive the Poco libraries
until `clickhouse_common_io` builds, and that is exactly what is still missing. Until then a
regression in the Poco Windows sources would only show up locally:

```bash
ninja -C build-windows -k 0 base/poco/Foundation/lib_poco_foundation.a
```

which compiles every Poco translation unit and then fails at the archive step. Poco joins the
gate on its own once `src/Common` builds - no change to `ported_targets.cmake` needed.

## Remaining work {#remaining-work}

What stands between the current state and a working `clickhouse-local.exe`, in the order it
is worth attacking:

1. **`src/Common` and `src/IO`.** The bulk of it. These reach for interfaces Windows does not
   have: `epoll`, `eventfd`, `timerfd`, `mmap`/`madvise`, POSIX signals and `siginfo_t`
   (`QueryProfiler`), `ucontext.h` (`StackTrace`), `/proc` (`AsynchronousMetrics`),
   `getrusage` (`ThreadProfileEvents`), `fork`/`exec` (`ShellCommand`), `O_CLOEXEC`, and
   `dlopen`. Each needs either a Windows implementation or an explicit, documented opt-out -
   a client does not need the query profiler.
2. **LLP64 fallout.** `-Wshorten-64-to-32` fires wherever a `size_t` meets a `long`-typed
   platform type, `off_t` being the most common. These are real narrowing conversions and
   each wants a look rather than a cast.
3. **`std::filesystem::path` conversions.** On Windows `path::value_type` is `wchar_t`, so
   `path` does not convert to `std::string` implicitly and the many places that return one
   from the other stop compiling. Deciding on one conversion helper - and on UTF-8 as the
   internal encoding - is a prerequisite.
4. **`MSG_DONTWAIT`.** Winsock has no per-call non-blocking flag; `SocketDefs.h` currently
   emulates it with a bit that is stripped before the call, and `SocketImpl::connectionOpen`
   polls with a zero timeout instead. `src/IO/SocketPeerClosed.cpp` needs the same treatment.
5. **replxx.** Its Windows console backend is intact upstream, but our fork broke it when it
   added support for custom descriptors: it writes to descriptors with `dprintf`/`fsync`
   outside any `#ifdef` and dropped the `tty::out` that `windows.cxx` reads.
   `contrib/replxx-cmake/replxx-windows-compat.h` patches around both; the fix belongs in
   [ClickHouse/replxx](https://github.com/ClickHouse/replxx).
6. **Runtime verification.** Nothing here has been *run* yet - only compiled and linked. The
   Wine smoke test above is the cheapest way in.

## CI {#ci}

The `Build (amd_windows)` job builds `clickhouse-windows-ported`. It is defined by
`BuildTypes.AMD_WINDOWS` in `ci/defs/defs.py`, its cmake flags in
`ci/jobs/build_clickhouse.py`, and it runs in the `clickhouse/binary-builder` image, which
installs `mingw-w64`. The job produces no artifact, because there is no binary yet; switch
its target to `clickhouse-bundle` once there is.
