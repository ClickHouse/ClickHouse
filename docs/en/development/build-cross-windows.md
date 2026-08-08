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
The Windows port **links but has never been run**. `clickhouse.exe` is produced by every CI
build, and nothing has yet executed a single instruction of it - the development host is
`aarch64` and its Wine has no `x86` emulator. Running it on an `x86-64` Windows machine is the
next step. [Remaining work](#remaining-work) is an inventory of what is left after that, by
subsystem.
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

mingw-w64 12 or newer is required: older releases (such as the 11 in Ubuntu 24.04) lack the
`_l`-suffixed per-locale CRT functions (`_iswctype_l`, ...) that the libc++ locale support
calls, in both the headers and `libmsvcrt.a`. If your distribution's package is older,
install the `mingw-w64-common` and `mingw-w64-x86-64-dev` packages from a newer release the
way `ci/docker/binary-builder/Dockerfile` does - they are data-only packages that install
cleanly anywhere - and point `-DMINGW_SYSROOT` at them if not at the default location.

## Build ClickHouse {#build-clickhouse}

```bash
cd ClickHouse
cmake -S . -B build-windows -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=$PWD/cmake/windows/toolchain-x86_64.cmake \
    -DENABLE_LIBRARIES=OFF \
    -DENABLE_TESTS=OFF \
    -DENABLE_UTILS=OFF \
    -DENABLE_CLICKHOUSE_ALL=OFF
ninja -C build-windows clickhouse
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

What builds for Windows today:

- the C++ runtime: libc++, libc++abi, LLVM libunwind (driving SEH on `x86_64`) and the
  compiler-rt builtins;
- 117 third-party libraries, among them OpenSSL, Boost, zlib-ng, zstd, LZ4, re2, Abseil,
  cctz, c-ares, replxx and Snappy;
- all of Poco - Foundation, Net, XML, JSON, Util and NetSSL;
- every library under `src`, and `programs`, which link into a native PE `clickhouse.exe`.

`clickhouse-windows-ported`, defined in `cmake/windows/ported_targets.cmake`, is the first two
of those. It is built alongside `clickhouse` rather than being subsumed by it, because it is
derived from the contrib list rather than from what the binary happens to link, so a
third-party library that nothing depends on yet stays covered.

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

## Remaining work {#remaining-work}

What stands between a binary that links and one that works.

**Run it.** Nothing here has been executed, only compiled and linked, so every runtime
assumption in it is untested. This is the one item that blocks all the others from being
meaningful. See the Wine note above: an `x86_64` PE binary needs an `x86_64` host or an
emulator such as FEX.

**Debug info.** The build passes `-g0` on Windows. A PE image cannot exceed 4 GiB, the DWARF
for this binary is several times that on its own, and PE has no `.gnu_debuglink` equivalent to
carry it in a separate file. A crash therefore symbolizes to a module and an offset, not to a
file and a line. Splitting the debug info out needs either a PDB writer or an out-of-band
format of our own.

**No `Epoll` backend.** `src/Common/Epoll.*` is Linux-only, so everything that polls sockets
through it - the parts of the client that wait on more than one connection at a time - does
not work yet. Windows has `WSAPoll` and I/O completion ports; the wake-up path already has a
Windows implementation in `WakeupFd`, built on a loopback socket pair because a Windows pipe
cannot be waited on alongside sockets.

**Subsystems that report `NOT_IMPLEMENTED`,** each with the reason recorded at its site: local
syslog, archive reading (`libarchive` needs a hand-written Windows `config.h`), conditional
writes to the local object storage (they need `flock` on a directory and sub-second
modification times), and the web terminal.

**The `server` mode is rejected up front.** `clickhouse server` prints that the server is not
supported on Windows and exits: its startup goes through `BaseDaemon`, which needs POSIX
signals and `fork`. The `clickhouse-server` alias is not installed. `clickhouse-server-lib`
itself is still built and linked, because `clickhouse-local` needs it.

**Compiled out, all server-side or POSIX-only:** the sampling query profiler and the signal
handlers (Windows reports faults through SEH instead), `ThreadFuzzer`, the `fork`-based
watchdog, `ShellCommand` and everything built on it, the pseudo-terminal features, and the
`su`/`docker-init`/`install` tools. Anything that needs one of these on Windows needs a
Windows design first, not a stub.

**`std::filesystem::path` conversions.** `base/base/pathToString.h` explains the problem and
is used at every site that needed it so far, but the codebase keeps relying on the implicit
`path`/`std::string` conversion that only exists on POSIX, so newly written code reintroduces
the errors. UTF-8 is the answer - `path::string()` is not, it goes through the active code
page.

## CI {#ci}

The `Build (amd_windows)` job builds `clickhouse` and `clickhouse-windows-ported`. It is
defined by `BuildTypes.AMD_WINDOWS` in `ci/defs/defs.py`, its cmake flags in
`ci/jobs/build_clickhouse.py`, and it runs in the `clickhouse/binary-builder` image, which
installs `mingw-w64`. The job publishes no artifact: the binary has not been run, so there is
nothing worth handing to anyone yet.
