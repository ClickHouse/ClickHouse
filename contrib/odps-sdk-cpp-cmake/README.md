# ODPS C++ SDK

ODPS (MaxCompute) C++ SDK, pulled in as a git submodule at `contrib/odps-sdk-cpp`
and built by the `CMakeLists.txt` next to this file.

    Remote:  https://github.com/aliyun/aliyun-odps-sdk-cpp
    Version: 0.50
    Base:    a1a7e541753f53970131e345a4f77581a8d6a9ac  (2026-08-31)
    Commit:  the gitlink at contrib/odps-sdk-cpp

This note lives here rather than in `contrib/odps-sdk-cpp` on purpose: the SDK
root is on the compiler's include path, so a file named `VERSION` there would be
found by `#include <version>` on case-insensitive filesystems.

## Submodule

The SDK is no longer vendored as plain files; it is a submodule pinned to the
commit above, so the source is fetched at build time like every other `contrib`
submodule:

    git submodule update --init contrib/odps-sdk-cpp

`contrib/CMakeLists.txt` mounts it with `add_contrib (odps-sdk-cpp-cmake
odps-sdk-cpp)`; `add_contrib` already skips the module when the directory is
missing or empty, so an uninitialized submodule degrades to "Build without ODPS"
instead of failing the configure.

The remote is the public GitHub repository of the SDK, reached over HTTPS,
so local development needs no credentials or scheme rewriting, just like
the other GitHub-hosted `contrib` submodules.

CI obtains the SDK through the repository's normal submodule initialization,
the same way as the other `contrib` dependencies.

The gitlink is the only exact revision source. The generated `sdk_version.h`
uses the stable integration label `0.50-clickhouse` because release source
archives do not carry nested git metadata.

The submodule is kept byte-for-byte identical to the official remote commit.
ClickHouse-specific compatibility and endpoint-policy integration live beside
this file and are compiled as part of the `_odps` target.

The official SDK translation units are compiled with `-w` because upstream is
not warning-clean under ClickHouse's `-Weverything -Werror` policy. That source
property is applied only to the explicit upstream source list; the
ClickHouse-owned adapter keeps the normal project diagnostics, while SDK public
headers are treated as system headers for that adapter.

## What is compiled

Only an audited explicit list of files from `common/`, `util/` and `tunnel/` is
compiled, plus the generated `sdk_version.h`. The rest of the submodule is
present but unused:

  - `core/` (ODPS REST metadata API) and `max_storage_api/` (MaxCompute Storage
    API) - nothing ClickHouse calls reaches them, and `core/` would additionally
    pull in the bundled `thirdparty/common/tinyxml2`.
  - `test/`, `example/`, `conf/`, `cmake/` and the SDK's own top-level
    `CMakeLists.txt` - the upstream build requires GCC >= 4.9.2 and fetches its
    own dependencies, so `contrib/odps-sdk-cpp-cmake/CMakeLists.txt` replaces it
    and links ClickHouse's `contrib` copies of protobuf, curl, OpenSSL, Arrow,
    lz4, zstd, zlib and Boost instead.
  - `util/crc32c.cpp` - excluded on purpose, see "CRC-32C" below.

The single-header dependency `<nlohmann/json.hpp>` is served from the submodule's
own `thirdparty/` directory, which is put on the include path.

## C++ standard

The SDK's own translation units compile as `-std=gnu++20` instead of the
project-wide C++23. The `gnu` dialect is required by the SDK's use of the
`typeof` GNU extension (`common/`, `tunnel/`), and C++20 is the floor the
pinned Arrow enforces: `tunnel/arrow_*.cpp` include Arrow headers whose main
paths use C++20 facilities such as `std::popcount` from `<bit>`, so a
`gnu++17` TU fails to compile against it (arrow-cmake itself rejects anything
below C++20).

## Compatibility layer

The SDK is written against older protobuf and Arrow versions than ClickHouse
uses. The SDK submodule remains unmodified: forwarding headers and narrowly
scoped forced includes adapt only the translation units affected by each API
change. There is no target-wide compatibility prelude.

  - **`google/protobuf/wire_format_lite_inl.h` no longer exists.** Later protobuf
    merged it into `wire_format_lite.h`, but the SDK's `tunnel/serialize.cpp`,
    `tunnel/arrow_record_reader.h`, `tunnel/arrow_record_writer.h` and
    `tunnel/coded_checksum.h` still include it. `compat/google/protobuf/wire_format_lite_inl.h`
    is a shim that forwards there. `compat/` is on the include path ahead of
    protobuf's and holds only this one file, so every other `google/protobuf/...`
    include falls through to the real headers.

  - **`CodedInputStream::SetTotalBytesLimit` lost its second parameter** in
    protobuf 3.11. `compat/odps_serialize_compat.h` is force-included only into
    `tunnel/serialize.cpp` and rewrites that one old call shape.

  - **The raw zlib constants `Z_BEST_COMPRESSION`, `Z_BEST_SPEED` and
    `Z_DEFAULT_STRATEGY` are no longer visible.** `util/gzip_util.cpp` and
    `tunnel/serialize.cpp` set `GzipOutputStream::Options` from these constants
    without including <zlib.h>; they relied on protobuf's `gzip_stream.h`
    leaking it (it used to declare `z_stream` members), which modern protobuf
    no longer does. `compat/odps_serialize_compat.h` and
    `compat/odps_zlib_compat.h` include `<zlib.h>` only for the two affected
    translation units. The linked zlib-ng is built in `ZLIB_COMPAT` mode and
    defines the constants with the classic zlib values, so behaviour is
    unchanged.

  - **`arrow::decimal(precision, scale)` no longer exists.**
    `tunnel/arrow_meta_helper.h` maps ODPS_DECIMAL through this factory,
    deprecated in Arrow 18.0 and removed since; the Arrow we build against
    (25.0) no longer has it. There it meant `precision <= 38 ? decimal128 :
    decimal256`, and ODPS decimals never exceed precision 38, so every
    reachable call produced a `decimal128`.
    `compat/odps_arrow_compat.h` rewrites the factory only for the three SDK
    translation units that include `arrow_meta_helper.h`. It deliberately does
    not use `arrow::smallest_decimal`, which could change the type seen by the
    columnar path for smaller precisions.

## ClickHouse adapter

`odps_clickhouse_adapter.cpp` is owned by ClickHouse and exposes one narrow
operation around the SDK's internal signed router request. `MaxComputeRaw`
first contacts the user-configured ODPS endpoint, validates the returned Tunnel
endpoint with ClickHouse's `RemoteHostFilter`, and only then constructs the
download against that direct endpoint. The resolved endpoint is kept for the
whole query, so parallel readers do not rerun routing and share one snapshot.

The SDK does not expose an in-flight cancellation hook. ClickHouse therefore
checks cancellation before and after SDK calls and applies explicit connect and
socket timeouts. A query already blocked inside one SDK request can complete
cancellation only when that request returns or times out.

Two workarounds the previous (0.42) vendoring needed are gone in 0.50, because
the SDK dropped the legacy `apsara` tree:

  - The `std::byte` vs Apsara `byte` collision, and the `-std=gnu++14` exception
    for `apsara/.../md5.cpp`, no longer apply: `md5` was rewritten on OpenSSL EVP
    under `util/md5.*` in namespace `apsara::odps::sdk::util` and uses `uint8_t`.
  - The `AlibabaCloud::Credentials` shim was removed: 0.50 ships its own
    lightweight `apsara::odps::sdk::Credentials` (`include/credentials_provider.h`)
    and has zero `AlibabaCloud` references.

## CRC-32C

`util/crc32c.cpp` implements a hardware CRC-32C with unconditional x86 `cpuid`
inline asm and SSE4.2 intrinsics. It is only reached through `tunnel/crc_32c.h`
when `TUNNEL_USE_CRC_32C` is defined, which we do not define. Without it the
tunnel uses `boost::crc_optimal<32, 0x1edc6f41, ...>` - the same CRC-32C, so the
wire format is unchanged. The file is absent from the explicit source list, so
the x86 asm never enters the build.

## Arrow support (`ENABLE_ODPS_ARROW`)

The SDK ships a Tunnel Arrow columnar reader/writer (`tunnel/arrow_*.cpp`, guarded
by `ODPS_SDK_ENABLE_ARROW`). It is built when `ENABLE_ODPS_ARROW` is `ON`, which
is the default whenever `ENABLE_ODPS_TUNNEL` is on and `ch_contrib::arrow`
exists (see the `option` block in `CMakeLists.txt`).

Two things make this option ABI-sensitive, and both are wired up already:

  - `ODPS_SDK_ENABLE_ARROW` inserts virtual functions (`GetArrowSchema`) into the
    middle of the `IDownload` and `IStreamUpload` vtables, so every consumer must
    be compiled with the same macro. The SDK target therefore defines it `PUBLIC`.
  - The target links `ch_contrib::arrow` `PUBLIC`, since `odps_tunnel.h`
    exposes `arrow::RecordBatch` in its public interface.

Since 0.50 every `tunnel/arrow_*.cpp` is internally guarded by
`#ifdef ODPS_SDK_ENABLE_ARROW`, and the Arrow call sites carry
`ARROW_VERSION_MAJOR >= 13` adaptation branches (`tunnel/arrow_util.h`). The
source-level filter in `CMakeLists.txt` is kept as a belt-and-suspenders step and
to keep the empty translation units out of the build when Arrow is off.

ClickHouse consumes this through the columnar read path of
`StorageMaxCompute` (see `src/Storages/OdpsArrowReader.*` and
`src/Storages/StorageMaxComputeArrow.*`, guarded by `USE_ODPS_TUNNEL &&
USE_ODPS_ARROW`). One protocol note: ZLIB is rejected by the Arrow reader
(`CompressOptionCompatWithArrow`), so the columnar path passes ZSTD when
`odps_read_compress` is on; the row-based path keeps using the configuration
default (ZLIB).

## Platform support

The previous x86-64-only limitation came from the `apsara` atomics header, which
is gone in 0.50. With `util/crc32c.cpp` excluded, no x86 inline asm or intrinsics
remain in the compiled sources, and the crypto/hash helpers (`md5`, `sha1`,
`hmac`) are implemented on OpenSSL EVP, so ODPS builds on aarch64 as well.

All ClickHouse-specific build glue lives in this directory: `CMakeLists.txt`,
`sdk_version.h.in` (replaces the upstream `cmake/configure_build_headers.py`
generator), the narrow adapter, and compatibility headers under `compat/`.
