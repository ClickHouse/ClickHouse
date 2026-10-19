# Building ClickHouse with Bazel

This port uses Bazel 9.3.0, hermetic-llvm, and `rules_rs`. Bazel fetches pinned
third-party sources, so a Bazel build does not require initialized Git
submodules. The compiler and LLVM libraries come from ClickHouse's LLVM fork
at `86b7fe802669a4513da4931d909c36f4728729b9`; the compiler is built from source.

```sh
bazel build --config=x86_64 //:clickhouse
bazel build --config=x86_64 //...
bazel test --config=x86_64 //:unit_tests_dbms
```

The `aarch64` configuration selects Linux ARM64. For native ARM64 compilers,
generators, and Cargo build scripts on BuildBuddy, use
`--config=cmake_match_aarch64`.

## BuildBuddy remote execution

Set up a BuildBuddy API key with remote execution permission in your user
`~/.bazelrc` or the ignored `.bazelrc.local`:

```bazelrc
common:remote --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
common:remote --bes_header=x-buildbuddy-api-key=YOUR_API_KEY
```

Then build the Linux x86-64 executable:

```sh
bazel build --config=remote --config=x86_64 //:clickhouse
```

The full `//:clickhouse` build analyzes over 400,000 targets, including the
source-built LLVM compiler. If the local Bazel server cannot hold that graph
and its fetched repositories, run Bazel itself on a BuildBuddy remote runner:

```sh
bb remote --runner_exec_properties=EstimatedMemory=32GB \
  --runner_exec_properties=EstimatedCPU=8 \
  --runner_exec_properties=EstimatedFreeDiskBytes=50GB \
  --host_jvm_args=-Xmx24g build --config=remote --config=x86_64 \
  --remote_download_outputs=minimal //:clickhouse
```

`remote` selects `//bazel:buildbuddy_linux_x86_64`, which inherits
`@llvm//:rbe_linux_x86_64` and uses BuildBuddy's Ubuntu 20.04 execution
image; `x86_64` selects the ClickHouse target platform. LLVM,
Rust, and code generators execute on Linux even when Bazel runs on macOS.
Bazel's `common:remote` configuration sets `--jobs=500` for remote builds
and enables the same remote configuration for `aquery`.
Bazel still evaluates repository rules and fetches source dependencies on
the client, so the client needs free disk space for the fetched repositories.
Only top-level outputs are downloaded. On macOS, the result is a Linux ELF
executable and cannot be run directly on the client.

Keep API keys out of version control. Existing BuildBuddy credentials in
`~/.bazelrc` also work; no additional local file is required in that case.

Dependency ownership, generation, and action-audit details are in
[`bazel/README.md`](bazel/README.md).

## Binary comparison

### Linux x86-64

On a Linux x86-64 host with BuildBuddy remote execution configured, build the
CMake reference with the compiler selected by this repository:

```sh
bash bazel/run_cmake_reference.sh
python3 bazel/refresh_cmake_metadata.py --arch=x86_64 \
  --build-dir="$PWD/build_bazel_reference_x86_64"
python3 bazel/refresh_cmake_metadata.py --check --arch=x86_64 \
  --build-dir="$PWD/build_bazel_reference_x86_64"
```

`bazel/run_cmake_reference.sh` builds `//bazel:cmake_toolchain_files`,
installs that compiler and its builtin headers, configures Release CMake with
the pinned submodules and Rust compiler, and builds the native CMake
`clickhouse` target. It records the compiler and executable SHA-256 values,
CMake File API replies, CMake trace, Ninja commands, and generated metadata in
`build_bazel_reference_x86_64`. It also uploads those files when run on a
BuildBuddy hosted runner. `CMAKE_REFERENCE_JOBS` controls Ninja on that Linux
host; it defaults to 12 and is independent of Bazel's `--jobs=500`.

To replay the CMake reference's embedded compiler paths, versions, flags,
and CMake version, and omit the extra `BUILD_SYSTEM` row, use:

```sh
bazel build --config=cmake_match //:clickhouse
```

For the stripped x86-64 comparison, run on a BuildBuddy Linux hosted runner:

```sh
bash bazel/run_stripped_hash.sh
```

`--config=cmake_match` expands to `--config=remote --config=x86_64
--//bazel:cmake_metadata`. This uses `common:remote` (`--jobs=500`), verifies that the Clang and Clang++
SHA-256 values match the recorded CMake toolchain, and builds `//:clickhouse`.
After Bazel finishes, it verifies the original CMake artifact in a temporary
directory outside the checkout. It first verifies the original
`llvm-strip --strip-all` hash, then compares copies made with:

```sh
llvm-strip --strip-all --remove-section=.note.gnu.build-id -o OUTPUT INPUT
```

Both copies must equal the recorded SHA-256
`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585`
and pass `cmp`. The comparison exits
at that point. LLVM's linker hashes the original ELF, including symbol and debug
tables, to create the build ID; retaining that ID also retains differences from
sections which stripping removes. The original ELF and its build ID are kept.
`stripped-summary.txt` records both original and plain-stripped hashes, the
comparison hashes, the strip flags, and `cmp` status.

The comparison passed in
[BuildBuddy invocation 10ea5019-d0e8-4f22-bb88-ef81fce6ac47](https://app.buildbuddy.io/invocation/10ea5019-d0e8-4f22-bb88-ef81fce6ac47).
Its [Bazel build](https://app.buildbuddy.io/invocation/7eb00bd2-e40d-4f8a-be7a-4554e4b53182)
completed 4,296 remote executions. The original CMake ELF and both selected
compilers were rehashed and verified against the recorded reference before
stripping. Results (SHA-256):

| SHA-256 | CMake | Bazel |
| --- | --- | --- |
| Selected Clang and Clang++ | `52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce` | `52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce` |
| Original | `cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc` | `0991da4fda873fdc3f7d5523b2a235734d4ad984ff586e51b2ab09147f724372` |
| `llvm-strip --strip-all` | `6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe` | `2ca4dbdd6bfd4511189c0547ebc62cb505cb1b760aa5346c553175ded61e5c93` |
| `llvm-strip --strip-all --remove-section=.note.gnu.build-id` | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` |

`cmp` of the last row returned 0, and `PARITY_STATUS=0`. This is stripped
binary equality; the original binaries and ordinary stripped build IDs differ.

The native-rule and dependency BUILD cleanup was compared with native CMake
after each batch. Every comparison below returned `PARITY_STATUS=0` and
`PARITY_STRIPPED_CMP_STATUS=0`. The normalized SHA-256 values were
`ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` on
x86-64 and
`a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` on
ARM64. The ARM64 CMake and Bazel compiler and tool verification is described
under [Linux ARM64 (native)](#linux-arm64-native).

| Cleanup batch | x86-64 comparison | Native ARM64 comparison |
| --- | --- | --- |
| Remove `cmake_native_compile_flags`; default native flags and features in `ch_cc_library` | [6d0f655f](https://app.buildbuddy.io/invocation/6d0f655f-2094-41f4-adb4-7e4c85298eb2) | [b897bd67](https://app.buildbuddy.io/invocation/b897bd67-c016-49a0-90c7-2a97d3df8e92) |
| Declare Boost, fmt, FlatBuffers and libuv public headers once through their owning dependencies | [0cce2c2a](https://app.buildbuddy.io/invocation/0cce2c2a-44d8-4ef2-a5ea-cedb599d06b5) | [166537b6](https://app.buildbuddy.io/invocation/166537b6-4b94-43d5-82d0-48773f9e3c83) |
| Collapse 414 single compile-group wrappers and preserve CMake archive and object order | [93c41ed5](https://app.buildbuddy.io/invocation/93c41ed5-3c29-4653-a4ac-616c612f27b6) | [888890f7](https://app.buildbuddy.io/invocation/888890f7-5597-4a4e-9097-c81f065910a1) |
| Deduplicate Arrow and dependency public headers, compact header globs and remove unused exported inputs | [f5bc803d](https://app.buildbuddy.io/invocation/f5bc803d-21d0-45a4-9004-e2c46ef0ca0e) | [6aae0e32](https://app.buildbuddy.io/invocation/6aae0e32-7f28-4e2f-afd5-584b6b695cc9) |
| Compile AWS, ORC, DMLC and ICU sources in their owning repositories, with public headers and the original object order | [abed9878](https://app.buildbuddy.io/invocation/abed9878-5651-4c75-9111-48896e062ff4) | [93f97ae5](https://app.buildbuddy.io/invocation/93f97ae5-982c-47fb-a6ef-79b79a288f86) |
| Declare 23 native header libraries, consume existing Arrow/librdkafka headers, and remove unreferenced external exports | [0637569e](https://app.buildbuddy.io/invocation/0637569e-2bd2-4310-940f-59172a308a1d) | [5f3aeea0](https://app.buildbuddy.io/invocation/5f3aeea0-8d18-4cee-b54d-83a2ca9f83da) |

In the final batch the original CMake and Bazel hashes, ordinary stripped
hashes, and selected compiler hashes also remained equal to their respective
values in the tables above and under [Linux ARM64 (native)](#linux-arm64-native).
All 159 external BUILD files were audited: the remaining export-only
repositories supply licenses, web resources or compressed language data;
their configured consumers do not compile C/C++ sources from them.

After using the recorded CMake compiler version in Arrow, Mongo C and Mongo
C++ generated headers, the same x86-64 hashes and `cmp=0` were verified in
[BuildBuddy 49f16579](https://app.buildbuddy.io/invocation/49f16579-c602-4eb5-8570-111469b2417a).

Moving `-O3`, `-DNDEBUG`, and `-gz=zlib` into `.bazelrc` and using
`ch_cc_library` for target-platform CPU flags and ClickHouse XRay/warnings
preserved every x86-64 hash in the table above. The full comparison in
[BuildBuddy b8794a76](https://app.buildbuddy.io/invocation/b8794a76-e24b-4778-8090-a04db10f8fa7)
([Bazel build 86eecebf](https://app.buildbuddy.io/invocation/86eecebf-47e3-4263-9f12-9f3e84f2d341))
returned `PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

Using common `--copt` for shared C/C++ flags and `-DOS_LINUX`, and
collapsing redundant CPU selections, preserved every x86-64 hash in the
table. [BuildBuddy d21148b0](https://app.buildbuddy.io/invocation/d21148b0-7ae9-4305-8e90-ba1fe0b9a758)
([Bazel build 9404f5b7](https://app.buildbuddy.io/invocation/9404f5b7-849b-4157-bb5b-6c8503712177))
returned `PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The `local_includes` and direct header/input cleanup preserved every
x86-64 hash in the table.
[BuildBuddy 3437e0b1](https://app.buildbuddy.io/invocation/3437e0b1-34fb-4abc-a822-96b01516a54a)
([Bazel build b7a16ab7](https://app.buildbuddy.io/invocation/b7a16ab7-5c75-4d7d-b581-f77247e39d6a))
returned `PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The simplified generated configuration was verified in
[BuildBuddy invocation c972df98](https://app.buildbuddy.io/invocation/c972df98-7d2d-4b65-ac42-5ebf5117b5a3)
([Bazel build](https://app.buildbuddy.io/invocation/7bbae2ed-c019-4a04-b07f-746a6570c815)).
The same compiler, full binary, plain-stripped and normalized hashes in the
table above all matched; `PARITY_STRIPPED_CMP_STATUS=0`, `PARITY_STATUS=0`.

The ARM64 execution and Cargo support preserved all x86-64 hashes in
[BuildBuddy invocation 2cc0b2e2](https://app.buildbuddy.io/invocation/2cc0b2e2-a9f5-49ea-ab98-706597095b65)
([Bazel build](https://app.buildbuddy.io/invocation/8a5fdc01-b656-4b65-8269-c064679fce4f)).
Both normalized files still passed `cmp`.

The common compile flags were moved to `.bazelrc` and removed from generated
native `cc_library.copts`. Clang C/C++ flags use `--conlyopt` and
`--cxxopt`, so PIE does not affect assembly. Cargo, NASM, and the recorded
LLVM runtime rules keep their flags because their custom actions do not read
`--copt`. ClickHouse's complete x86-64 and ARM64 Bazel binaries were unchanged
by the flag cleanup. These ARM64 checks preceded the native CMake ClickStack
and SQLConsole resource metadata update:
[x86-64 13-flag build](https://app.buildbuddy.io/invocation/fae9a5a7-fb8e-411f-bfe9-e19b133e0b51)
and [ARM64 13-flag build](https://app.buildbuddy.io/invocation/dceb77aa-cc78-42b9-8fd7-f3d8cf044972).
The x86-64 build again passed `cmp` against normalized CMake, with every
hash in the table above unchanged.

After moving C/C++ `-Werror` to `.bazelrc` and sharing the repeated
ClickHouse warning list,
[x86-64](https://app.buildbuddy.io/invocation/92c74fe8-b381-45f1-b8bb-eed31254ee80)
again preserved every compiler and binary hash in the table above and passed
`cmp` against normalized CMake (`PARITY_STRIPPED_CMP_STATUS=0`).
[ARM64](https://app.buildbuddy.io/invocation/f5560883-a6a5-40db-b2a8-2e7665b28859)
preserved the then-current complete Bazel binary hash
`da5a37c6f1f40b8026b577ba3fd36e8e26a7cc4bd3ba073e77f007077b02d52a`.
The two glibc compatibility C compilations retain their CMake `-Wno-error`
exception. Contrib `-w` stays on targets that already disable warnings.

LLVM's stage0 startup objects clear application C flags in their existing
transition. That restores the three ARM `crtbegin*.o` files to
`d923570ce208352e2b654f2784c31ff8a7e3aa4ea9b039a9f86f977850028f03`
without changing the complete ARM Bazel binary:
[ARM64 compiler, CRT and binary checks](https://app.buildbuddy.io/invocation/e24a290d-9898-43b5-9d31-2e4df4a31067).
The [x86-64 CMake comparison](https://app.buildbuddy.io/invocation/09b3baad-f52d-49c7-8332-bef11ddadeac)
again preserved all hashes in the table and passed `cmp`.

On the original Linux x86-64 reference host, run the complete audit with:

```sh
bash bazel/run_bazel_parity.sh
```

Use `bash bazel/run_bazel_parity.sh --stripped` for the same normalized
comparison against a CMake build retained on that host. Both entry points use
`bazel/verify_stripped_hash.py` and record plain-stripped hashes separately.

`bazel/run_bazel_parity.sh` verifies the saved reference hashes and metadata,
rebuilds `//bazel:cmake_toolchain_files` and confirms that Bazel selected the
recorded CMake compiler. It also compares all embedded LLVM generated headers
against the CMake headers. It then builds the Bazel executable and seven Rust
link archives, records both complete ELF hashes, the `cmp` result and all
seven native/Bazel archive hashes,
and compares the CMake File API and Ninja commands to Bazel `aquery` with
EngFlow/any2bazel. It retains the reports in the CMake build directory and
uploads the reports on a BuildBuddy hosted runner. Its status distinguishes a
binary or command difference from a failed build or failed audit.

### Linux ARM64 (native)

For ARM64, run these commands on a BuildBuddy Linux hosted runner:

```sh
bash bazel/run_native_reference.sh --configure-only
bash bazel/run_native_reference.sh
python3 bazel/refresh_cmake_metadata.py --arch=aarch64 --full-reference \
  --build-dir="$PWD/bazel-bin/bazel/cmake_reference_aarch64"
bash bazel/run_native_reference.sh --compare
```

`run_native_reference.sh` prepares the current Git checkout, its pinned submodules, and Ubuntu ARM64
tools as declared Bazel inputs. The checkout uses each Git commit's timestamp
so ClickHouse's embedded gzip resources are reproducible on another worker.
`//bazel:cmake_reference_aarch64` runs ClickHouse's original CMake on a native
ARM64 Linux BuildBuddy executor with the same source-built compiler as Bazel.
CMake builds its TableGen tools on ARM64 and the reference checks that the
compiler, Rust tools, TableGen tools, and executable are all ARM64 ELF files.
It records the original, plain-stripped, and normalized CMake hashes and the
compiler hash in `cmake-reference-summary.json`.

The native ARM64 reference inputs below were measured at source commit
`9906f9142ae9b16d8d1da7354907dd9727758dd7`, on
`refs/heads/bazel-buildbuddy`, with the Bazel changes under validation.
The source archive includes the real Git metadata read by ClickHouse CMake.
On later commits, refresh the CMake metadata before comparison as shown above;
the newly embedded Git commit can change the binary hashes. These input hashes
are recorded separately from binary validation:

| Input | SHA-256 |
| --- | --- |
| Source archive at `9906f9142` | `45fc5be13cbb7a94e4e2c0a99cd8e11a6f088c1df057af4c0cdfee0700e56cf3` |
| ARM64 tools archive | `be979691db973446e80c1ba08d4fa0843fcd64fe8a02b1bc377208b0cc69d3af` |
| Source-built ARM64 compiler | `3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f` |
| Ubuntu 22.04 OCI image index | `0a9887931b0e38af2e3487ed39e29436b0535c4f3f52e57745baaf79d1898956` |
| Ubuntu 22.04 Linux ARM64 OCI manifest | `88881eb36da489f9ff8746ece8048ca16cc94f80e8cd3ae7e8335d8822631591` |

The archive hashes were checked by
[BuildBuddy f91755ca](https://app.buildbuddy.io/invocation/f91755ca-f16c-4fc9-96ff-bf44cbe32ae5);
the compiler hash was checked in
[BuildBuddy e24a290d](https://app.buildbuddy.io/invocation/e24a290d-9898-43b5-9d31-2e4df4a31067).
The ARM image pin preserves the entire native CMake input tree: old action
`118da31bd42d0b12ca605d11c859ef8a29d6bc5f6c31651d76cb0790692afcd9`
and new action
`a8da5e91e8557207dd2a01e6ae3ea94b977d3c687d84d835a137647a91548bc0`
both use input root
`1017566ce6eaeff728f2f3a658124a66f647afdd3f4b58abfa5c0e282c48b8e7`.
This includes the native Clang, Rust, source archive, and utility archive.
These hashes do not establish binary parity.

The full native CMake reference completed in
[BuildBuddy 12df76fc](https://app.buildbuddy.io/invocation/12df76fc-3a68-46da-8af4-1e01c1dd4b4b);
its status is `{"status":0,"phase":"built","configure_only":false}`. The ARM64
comparison in [BuildBuddy 7a855e69](https://app.buildbuddy.io/invocation/7a855e69-ff30-4b33-92b0-7c61ed6b6517)
([Bazel build 77a7cbb6](https://app.buildbuddy.io/invocation/77a7cbb6-6430-42fd-8f84-cef21677368e))
measured the following hashes. Its `ClickHouseNativeLink` action
`c3edb5788f2bebe5a5c5cb321126bef5ef14addf019a94abb19672eb004efdbb`
also records the final Bazel executable's SHA-256 and size.

| ARM64 SHA-256 | CMake | Bazel | Bytes |
| --- | --- | --- | --- |
| LLVM compiler / `llvm-strip` | `3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f` | `3c685a0e2448e17c0c11e265a7027c2cfaca6158018220a67ca2af9d3df0e71f` | Both 221,377,856 |
| Original ELF | `cd204de2504bbe77c5e2b0babce701943133374e77490f36ad8b679d18b56646` | `2363f1ffebe756464876c0e91ca336d77c4d17e6a27afe2fb22b8f337ac2d744` | Both 907,067,440 |
| `llvm-strip --strip-all` | `5180f5d1ee2918cccf2e872ec9c92a2e8de8c1021e75972ad1ba2df53097b88a` | `5dbc0eee809acefa84bf3095453cbd684b5ce8803a8dee77ce19704887413c5a` | — |
| `llvm-strip --strip-all --remove-section=.note.gnu.build-id` | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | Both 637,316,696 |

The normalized copies passed `cmp`: `PARITY_STRIPPED_CMP_STATUS=0` and
`PARITY_STATUS=0`. Both are 637,316,696 bytes and match byte for byte.
Clang, Clang++, Rust, Cargo and the reference TableGen tools are native ARM64
ELFs. The original and plain-stripped binaries differ because their build IDs
still include information from discarded sections.

The common `--copt`, `-DOS_LINUX` and CPU selection cleanup preserved
every ARM64 hash in the table.
[BuildBuddy 7978c989](https://app.buildbuddy.io/invocation/7978c989-9762-46ce-96e7-956313e499fe)
([Bazel build 26aa0801](https://app.buildbuddy.io/invocation/26aa0801-f625-4be7-9cdb-f4196ebb15ab))
rechecked the native CMake reference and returned
`PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The `local_includes` and direct header/input cleanup also preserved
every ARM64 compiler, original ELF, plain-stripped and normalized hash
in the table.
[BuildBuddy 9539db98](https://app.buildbuddy.io/invocation/9539db98-6d61-483b-8796-d775d898288e)
([Bazel build 079cc9b2](https://app.buildbuddy.io/invocation/079cc9b2-600f-49c4-950a-1a18c326e531))
verified the native CMake reference and returned
`PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

The subsequent `ch_cc_library` and common optimization flags also preserved
all ARM64 compiler, original ELF, plain-stripped, and normalized hashes in
the table above. The full native comparison in
[BuildBuddy fd0db6c9](https://app.buildbuddy.io/invocation/fd0db6c9-0b22-4a3e-b2cd-83a0b9a0dacc)
([Bazel build d92f2e6c](https://app.buildbuddy.io/invocation/d92f2e6c-4a81-4674-8f56-74bcf2d18f68))
reused and checked the recorded native source and tools archives, then returned
`PARITY_STRIPPED_CMP_STATUS=0` and `PARITY_STATUS=0`.

<details>
<summary>Earlier ARM64 measurements and the compiler metadata fix</summary>

Before refreshing the MongoDB compiler version, the Bazel executable in
[BuildBuddy f91755ca](https://app.buildbuddy.io/invocation/f91755ca-f16c-4fc9-96ff-bf44cbe32ae5)
had original SHA-256 `696185b7d3351b5e171cdc14917d39fa99bec07b4812445b67e01572a00d1c3b`
(907,067,440 bytes), plain-stripped SHA-256
`291a3ceb0cd0e93e8742f3a7ff8283b3475f6a0265469ac362b34e56073ca879`
(637,316,784 bytes), and normalized SHA-256
`469ba31fac67a8e3f4f5c3f48e974dcc6b09c557a1b37249a1d4a170818aed9f`
(637,316,696 bytes). This first comparison returned `PARITY_STATUS=1`.
The [ARM strip action](https://app.buildbuddy.io/invocation/f647cac4-6d5d-4feb-8ccb-41c3dc80ead5)
independently verified that earlier binary and `llvm-strip` on ARM64. Its
hosted runner retained the measured
`bazel-arm64-stripped.json` in
[BuildBuddy dbff081e](https://app.buildbuddy.io/invocation/dbff081e-ea3b-41fc-b9f6-9585da096dbd).

That earlier Bazel executable also passed a native ARM64 runtime check in
[BuildBuddy 326c1056](https://app.buildbuddy.io/invocation/326c1056-fdbd-4f24-abd2-d3a6767a7b26)
([ARM64 action](https://app.buildbuddy.io/invocation/793dae40-7204-4329-876c-8aa3ca021ef3)).
The ARM64 executor rechecked that earlier SHA-256 and byte count.
`clickhouse local --version` printed `ClickHouse local version 26.11.1.1.`,
and `clickhouse local --path=<scratch> --query='SELECT 1'` printed exactly
`1\n`; both exited 0. This check predates the final compiler-metadata fix.

The first mismatch started at byte 105,961 in `.rela.dyn`. The only
`.rodata` content differences were two MongoDB strings recording Clang
`21.1.8` instead of native CMake's `22.1.1`. All 161,684 differing
relocations referenced the same strings at addresses shifted by those
compiler strings. Mongo C, Mongo C++ and Arrow now obtain the C++ compiler
version from `bazel/cmake_metadata.bzl`; `bazel/generate_build.py` preserves
that behavior on regeneration.

The first ARM64 Bazel build completed 17,334 remote actions in
[BuildBuddy invocation 8952f682](https://app.buildbuddy.io/invocation/8952f682-f072-4073-b1e4-bb9c88abbfa2)
([Bazel build](https://app.buildbuddy.io/invocation/9da0fe08-e021-4171-8c38-b840fa023626)).
Its original executable SHA-256 was
`ccef0e7abc7d89b76b5c9b5d8680c6fefc86edc3b19d2280503e96fdfacff173`.
After refreshing the compiler, Git, and license metadata from native ARM64
CMake, and before refreshing the ClickStack and SQLConsole resource timestamps,
Bazel built the original executable
`da5a37c6f1f40b8026b577ba3fd36e8e26a7cc4bd3ba073e77f007077b02d52a`
in [BuildBuddy invocation 9124c824](https://app.buildbuddy.io/invocation/9124c824-09d1-4f34-b553-e77d79569197)
([Bazel build](https://app.buildbuddy.io/invocation/42467401-4875-450b-b712-9bf686f51fdc)).
Those two values record earlier Bazel binaries; they do not establish
stripped equality with the completed native ARM64 CMake reference.
</details>

`--compare` uses `//bazel:stripped_hash_aarch64` to verify the recorded
hashes and compare normalized copies of the actual CMake and Bazel binaries.
The CMake executable is an input only to the comparison, never to
`//:clickhouse`. The ARM64 configuration retains `common:remote` and
`--jobs=500`; CMake's Ninja build uses 12 jobs on its ARM executor.

### Embedded CMake metadata

`--//bazel:cmake_metadata` uses the values recorded by
`bazel/generate_build.py` or refreshed by `bazel/refresh_cmake_metadata.py`
from the configured CMake reference. It also maps generated source filenames
to the corresponding CMake build directory, so compiler-generated filenames
such as `__FILE__` match that reference. The reference must match the CMake
build being compared. Without the flag, `system.build_options` reports the
actual Bazel toolchain and flags, and generated filenames use Bazel directories.
The metadata generation has been executed in both modes; with the flag, its
output matches the configured CMake metadata source byte for byte.

<details>
<summary>Earlier Linux x86-64 reference and build history</summary>

These results record the earlier builds that led to the verified x86-64
normalized comparison above. Earlier unequal results are not the current
comparison result.

The Linux x86-64 CMake reference linked successfully in
[BuildBuddy invocation 4ea26abf-d8ee-4634-9d07-458a3645f3bc](https://app.buildbuddy.io/invocation/4ea26abf-d8ee-4634-9d07-458a3645f3bc).
The SHA-256 of `build_bazel_reference_x86_64/programs/clickhouse` is:

```text
cfbf99ef585fc2dbcf9f9ca413e13230ba2c9ab7dc8518df8bdeab06aa087cbc
```

The executable has 929,619,736 bytes. BuildBuddy's uploaded
`clickhouse.cmake` artifact has the same digest as the
`cmake.sha256` produced by the build. This reference used source commit
`f61e9b32a64778e7949ae174be73b7950a52527f` with its pinned submodules,
and the LLVM 22.1.1 stage-1 compiler built by `//bazel:cmake_toolchain_files`
from ClickHouse LLVM commit `86b7fe802669a4513da4931d909c36f4728729b9`.
The compiler SHA-256 is
`52a7d0fdc59f6a0cd416844e55a70f67e689eeb1df0d98cc8c018444eb6262ce`.
The CMake build was Release with libraries, Rust, tests and ClickHouse ALL
enabled, compiler cache disabled, `SOURCE_DATE_EPOCH=1`, the repository's
`cmake/linux/toolchain-x86_64.cmake` sysroot, CMake 3.28.3 and Rust
`nightly-2026-03-22`. The exact invocation is in the uploaded
`configure-command.json`; `bash bazel/run_cmake_reference.sh` reproduces
the setup and build on a Linux x86-64 host with BuildBuddy configured.

This is the reference for commit `f61e9b32`, including after PR #1 was
rebased to `4213ffa205c2ab7e99e638e76626b166e12a992a`. Before the
CMake-ordered Bazel link and the separate ClickHouse LLVM library targets
were added, the rebased Bazel build with `--//bazel:cmake_metadata` produced:

```text
14e03f0d21619b51a1b9ecc8b4c128ffb29007966f4a000d4d93b8abea8bd418
```

That build completed in
[BuildBuddy invocation 038e6ee0-4b67-406f-b464-ea6f317f20cc](https://app.buildbuddy.io/invocation/038e6ee0-4b67-406f-b464-ea6f317f20cc).
The comparison in
[BuildBuddy invocation 92e53e41-9f66-42ca-87f1-defc308f033a](https://app.buildbuddy.io/invocation/92e53e41-9f66-42ca-87f1-defc308f033a)
rehashed the preserved CMake ELF as `cfbf99ef…087cbc` and `cmp` returned 1.
The rebased Clang, Clang++, LLVM ar and linker driver still all hash to
`52a7d0fd…6262ce`, verified in
[BuildBuddy invocation c5256bb3-ae51-4b71-9491-a2feec3e0fda](https://app.buildbuddy.io/invocation/c5256bb3-ae51-4b71-9491-a2feec3e0fda).
The original link did not reproduce the CMake binary.

The first Bazel build using the CMake archive order, actual Bazel-compiled
objects and separate ClickHouse LLVM library targets then linked successfully:

```text
b5f8f0e61dc44ecdc257202f85d8d731924b7e63f5e52d1de2b2cf9ad1700b38
```

[BuildBuddy invocation 2e7d3927-c264-406d-8b0e-27077a7f1700](https://app.buildbuddy.io/invocation/2e7d3927-c264-406d-8b0e-27077a7f1700)
completed 58,060 actions with 4,354 remote executions. The complete ELF SHA-256
is preserved in the `bazel.sha256` artifact of
[BuildBuddy invocation 71bc2804-e383-47ce-a991-e74e472c5bba](https://app.buildbuddy.io/invocation/71bc2804-e383-47ce-a991-e74e472c5bba).
This build predates the Release frame-pointer and copied-source filename fixes.
Its hash differs from the recorded CMake hash, so matching the LLVM sources and
archive order alone did not reproduce the executable.

After separating the ClickHouse LLVM libraries from the compiler build, the
comparison in
[BuildBuddy invocation 5f7fd5ec-0af7-4465-be86-335619767563](https://app.buildbuddy.io/invocation/5f7fd5ec-0af7-4465-be86-335619767563)
confirmed that both compilers still match the recorded compiler SHA-256 and all
59 generated LLVM headers match CMake byte for byte. Its
[Bazel build](https://app.buildbuddy.io/invocation/cfb35394-b1ac-4f90-bc73-4a2d546303fb)
linked successfully after applying the CMake Release flags to ClickHouse
compile groups and produced:

```text
380d0fe4326fd38ff7f0302c6ed08abf6aa11417f6a1baccebe2e01375a6faa0
```

`cmp` against the rehashed CMake ELF returned 1. This build preceded the final
correction of CMake source-prefix maps, including the empty replacement CMake
actually used. LLVM header identity and matched Release flags therefore still
do not establish executable equality.

The build with the recorded empty CMake source-prefix replacement and
metadata-mode generated filenames also linked successfully:

```text
526bcee78b28c0acecfbfbcb4967200997c7934f497fc9ba4a541141bb1a887a
```

[BuildBuddy invocation 7f3c8e95-a8e8-4182-9788-b390673b9bf3](https://app.buildbuddy.io/invocation/7f3c8e95-a8e8-4182-9788-b390673b9bf3)
completed with 16,479 remote executions. Its full `bazel.sha256` is recorded in
[BuildBuddy invocation 55cb8cd4-84e6-48f0-ac49-fe289e891947](https://app.buildbuddy.io/invocation/55cb8cd4-84e6-48f0-ac49-fe289e891947).
This is a distinct complete ELF hash from the CMake reference, so source-prefix
and Release-flag parity still do not establish executable equality.

The next build replaced six imported bootstrap archives with the exact CMake
`cxx`, `cxxabi`, `unwind`, `clang_rt_builtins`, `clang_rt_xray`, and
`libllvmlibc` compile groups. The focused
[Bazel runtime build](https://app.buildbuddy.io/invocation/48f1710d-578c-4a16-b7d9-074f65a06c9a)
succeeded with 364 remote compiles. Its
[command audit](https://app.buildbuddy.io/invocation/8d49c7d6-3122-45c6-a2c0-5f21f6c65f03)
verified all six CMake source counts, both selected compiler SHA-256 hashes,
and absence of bootstrap libc++ include directories in those compile actions.
The 125 AArch64 outline-atomic sources are generated from the `lse.S`
symlinks verified against the saved Ninja commands. That check validated runtime
inputs and did not compare the complete executable.

The full Bazel build with all six native runtime libraries completed in
[BuildBuddy invocation 865f8d87-e8df-47c2-904d-b2d7cc513cf7](https://app.buildbuddy.io/invocation/865f8d87-e8df-47c2-904d-b2d7cc513cf7).
It executed 450 remote actions, linked all 411 validated archive/object
references and produced:

```text
3cbde2adf20b849b9e29d8ea40c47d4c5ab997fe1ca3f2af55f17476ad74f556
```

[BuildBuddy invocation 6ec60425-63bd-47e3-a6f5-891cab529e39](https://app.buildbuddy.io/invocation/6ec60425-63bd-47e3-a6f5-891cab529e39)
rechecked the original CMake SHA-256 as `cfbf99ef…087cbc`, both compiler
SHA-256 values as `52a7d0fd…6262ce`, and all 59 generated LLVM headers.
`cmp` returned 1. ELF capture found 46 native sections and 47 Bazel sections,
with 45 differing table entries or contents; that build did not reproduce the
CMake binary.

[BuildBuddy invocation 5d1cb4eb-a1f6-4ee7-ace6-f2e3a19d39b4](https://app.buildbuddy.io/invocation/5d1cb4eb-a1f6-4ee7-ace6-f2e3a19d39b4)
subsequently verified 122 actual compile actions for PostgreSQL, ClickHouse
glibc compatibility, and allocation interceptors with the pinned ClickHouse
sysroot. The compiler hash still matches the CMake reference.
Both CMake's and Bazel's `src/port/noblock.c.o` hash to
`a36c33b4e4abcd560d5a6f4e1d908a17e061c9c4baa80545a3476e416b30b321`.
This verifies that object, not the complete executable.

The full build using that pinned sysroot and native runtime sources completed
in [BuildBuddy invocation c6b645aa-9fb0-44d5-8394-d97a1accb134](https://app.buildbuddy.io/invocation/c6b645aa-9fb0-44d5-8394-d97a1accb134).
It ran 16,934 remote actions, with 40,950 action-cache hits, and produced
this complete Bazel executable SHA-256:

```text
8f4893576486f640d0d419d5654d6d87224445c6c8c6c5dede1291de98fd16d4
```

[BuildBuddy invocation 6be6d683-3a3b-4b47-9ac0-8301aa22a5fe](https://app.buildbuddy.io/invocation/6be6d683-3a3b-4b47-9ac0-8301aa22a5fe)
verified the original CMake executable and both compilers before this build.
`cmp` returned 1. Relative to CMake, Bazel's `.text` was 1,621,680 bytes
larger and `.rodata` was 694,702 bytes larger. This build used the earlier
Rust rules; it did not contain the Cargo corrections described below.

[BuildBuddy invocation 3f72be06-7136-445b-8ce4-d74d73deac15](https://app.buildbuddy.io/invocation/3f72be06-7136-445b-8ce4-d74d73deac15)
compiled PCO, PRQL, and Polyglot with the pinned Cargo and Rust compiler
from Bazel's declared source and vendor inputs. The
[comparison](https://app.buildbuddy.io/invocation/3f203571-a2c7-4490-80ff-87972e24bffd)
rechecked the original CMake executable and both Rust tool hashes. All three
raw Cargo archives and all three processed link archives match byte for byte,
including archive member order and names:

| Archive | Raw Cargo SHA-256 (both builds) | Processed SHA-256 (both builds) |
| --- | --- | --- |
| PCO | `4ff85afd197267e797443c9418b647fdf980ac6620a17d215349ae75fdf9988b` | `43a5186e80e9fa8e8132c7f1bb933fe717e8d1ef71e7b3f429b48d9f561b02cf` |
| PRQL | `c8be19bad2d042c0bfa2ca594f6f1b9b543aeb3e1cf8ac547d891ee5ccd9f725` | `6b585395d4779325864febc38f564b5ce4d98c55a655c4e92d1b12ede29362f7` |
| Polyglot | `6dc134152f87338fe80ed924497fe64c2439e751d3b531ad895b6623817e9cd3` | `309f9f7e0e9232bb7c3c705b70dc1fd6186805038a5e95515544d51f1bec8811` |

Cargo derives member filenames from the exact `RUSTFLAGS` argument vector,
including split `-C` arguments and the literal sysroot directory. PRQL's
build script also embeds the pinned rust_vendor revision. The Bazel Cargo
rule reproduces both from the repository's source and toolchain data.
Archive equality does not by itself establish complete executable equality.

The full Bazel build using all seven Cargo rules and the recorded CMake
assembly include directories succeeded in
[BuildBuddy invocation 2fa143e3-2820-4ce7-aefd-ac779d056316](https://app.buildbuddy.io/invocation/2fa143e3-2820-4ce7-aefd-ac779d056316).
The complete unstripped Bazel executable SHA-256 is:

```text
871e8a1a4a665e91261ef47a6192d85f972962feabf24431c0ca4d57caafce63
```

[BuildBuddy invocation 9b87cd8e-2a38-46ea-a03b-6f508b636a33](https://app.buildbuddy.io/invocation/9b87cd8e-2a38-46ea-a03b-6f508b636a33)
verified the original CMake hash, both compiler hashes, and all 59 LLVM
generated headers before building. `cmp` returned 1. Both executables have
46 ELF sections; relative to CMake, Bazel's `.text` is 20,544 bytes smaller,
`.rodata` 4,096 bytes smaller and `.eh_frame` 568 bytes smaller. The
`.gcc_except_table`, `.data.rel.ro`, `.data`, and `.bss` sizes now agree.
`.bss` contents also agree. The complete executable still differs.

The comparison confirmed exact SHA-256 equality for the processed Skim,
PRQL, Polyglot, and PCO archives. Wasmtime, Delta Kernel FFI, and chdig now
have identical ordered archive member names, but their processed archives
differ by -890, +1,152, and +20,852 bytes respectively. The native command
audit in this run stopped before producing its report because the separate
NASM and C++ aquery results both contained the final link. This does not
alter the ELF hashes above. The audit now coalesces duplicate actions only
when the Bazel action key, mnemonic, label, outputs, command, environment and
resolved input filenames agree.

PR #1 was subsequently rewritten at
`29281120601f767e8e828f57ec9baf35ca898674`. The branch contains that PR head.
Its five-file update changes Bazel's LLVM flags and Zstd headers; no native
ClickHouse source files or pinned submodules changed from `4213ffa205c`.
The original CMake executable SHA-256 above remains the reference. A
[focused Rust comparison](https://app.buildbuddy.io/invocation/db441cd8-f203-43c1-901c-66d8e47eb08e)
confirmed equal archive member names and ordering for the three remaining
archives. All remaining Delta members have identical allocated sections; only
Ring debug filenames differ. Wasmtime and chdig retain different paths to
generated Rust files in Cranelift and perfetto_protos. Corrosion's Cargo
directory calculation has now been reproduced.

On PR head `292811206`,
[Bazel invocation 1143155c](https://app.buildbuddy.io/invocation/1143155c-1259-4055-8ecd-dcd8f831362d)
built successfully after 12,382 remote executions. The original CMake hash and
both compiler hashes were reverified. All 59 generated LLVM headers matched.
The full Bazel executable SHA-256 is:

```text
008c496b0bca4f4e936a40b93748c90d672dc46f313329aee2522239e94875de
```

`cmp` on the complete executables returned 1. Both ELF files contain 46
sections; Bazel's `.text` is 20,544 bytes smaller and `.rodata` is 4,096
bytes smaller. Generated Cargo paths changed the full hash but did not change
these section-size differences. Those differences concern code and read-only
data, so stripping debug information alone cannot remove them.

The first plain-stripped comparison on that PR head was
[BuildBuddy invocation 82a328c8](https://app.buildbuddy.io/invocation/82a328c8-c943-4a0a-923e-21b63a3d07d6).
The unstripped CMake and Bazel hashes remain the hashes above. After
`llvm-strip --strip-all -o COPY ORIGINAL`:

| Input | Stripped executable SHA-256 |
| --- | --- |
| CMake reference | `6cd5519a79a8bc67fd3e1277af216b2dc327b045ad36abcf7535c50af89ff6fe` |
| Bazel PR 292811206 | `fe6d2a4f996d03e4bc4f61911e11f793d9caed8a15b0fdc259121ba91b6c06bc` |

The stripped `cmp` returned 1. Both stripped files have 43 ELF sections and
retain the 20,544-byte `.text` difference. The complete symbol-size comparison
reports 777,259 equal function-size multisets and 65 unequal ones; all 65 are
LZ4 functions. The Rust symbols absent on one side have matching sizes and
differ in LLVM-generated local symbol suffixes. The symbol report and original
ELF files diagnose the remaining difference; they do not establish equality.

The CMake link now compiles `@clickhouse_lz4//:_lz4__compile_0` from the
four pinned `contrib/lz4/lib` source files. The CMake flags include
`LZ4_FAST_DEC_LOOP=1` and omit the BCR-only `XXH_NAMESPACE=LZ4_`.
`lz4hc.c` also receives `lz4.c` as a declared compilation input.
[Bazel invocation bf4791f5](https://app.buildbuddy.io/invocation/bf4791f5-3eb7-44a3-b16d-0e0653dacd6c)
completed with 5 remote executions and 24,754 remote cache hits. Its hashes are:

| Input | SHA-256 |
| --- | --- |
| Bazel full executable | `c263799280b379235b90596b8005d0e8b799e62e88c805f52fa8454c6b4b2a3f` |
| Bazel `llvm-strip --strip-all` executable | `73db3edaac7436ef7f07e06175d74ad33c37dd7219d12bd896610d79a7f51ad8` |

The selected compiler still matched `52a7d0fd…6262ce`. This earlier
plain-stripped hash differed from the reference hash. The current normalized
comparison and its reproduction commands are recorded above.

</details>
