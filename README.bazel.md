# Building ClickHouse with Bazel

`//:clickhouse` builds the Linux executable for x86-64 or ARM64. Use Bazel
[9.3.0](.bazelversion). Bazel fetches the pinned third-party sources and builds
[ClickHouse's LLVM 22.1.1 compiler](bazel/llvm.MODULE.bazel) from source;
a Bazel build does not need initialized Git submodules. Run commands from the
repository root. On a Linux x86-64 host:

```sh
bazel build --config=x86_64 //:clickhouse
bazel test --config=x86_64 //src:unit_tests_dbms
```

The default is `-c opt`: ClickHouse C and C++ targets use `-O3` and define
`NDEBUG`. Use `-c fastbuild` or `-c dbg` to keep assertions enabled.

## BuildBuddy remote execution

Use a BuildBuddy API key with remote execution permission. Put these entries
in `~/.bazelrc` or the ignored `.bazelrc.local`, replacing `YOUR_API_KEY`:

```bazelrc
common:remote --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
common:remote --bes_header=x-buildbuddy-api-key=YOUR_API_KEY
```

```sh
bazel build --config=remote --config=x86_64 //:clickhouse
bazel build --config=cmake_match_aarch64 //:clickhouse
```

[`common:remote`](.bazelrc) sets `--jobs=500`, uses the BuildBuddy remote
executor and cache, and disables local fallback for remote execution.
`x86_64` and `aarch64` select the target CPU. The `remote` configuration
selects the Linux x86-64 execution platform; `cmake_match_aarch64` adds
native Linux ARM64 compiler and test platforms and selects the ARM64 target.
The client still analyzes the build and fetches repositories. To run Bazel
itself on a BuildBuddy runner:

```sh
bb remote --runner_exec_properties=EstimatedMemory=32GB \
  --runner_exec_properties=EstimatedCPU=8 \
  --runner_exec_properties=EstimatedFreeDiskBytes=50GB \
  --host_jvm_args=-Xmx24g build --config=remote --config=x86_64 //:clickhouse
```

Remote builds use `--remote_download_outputs=minimal`. Bazel reads Clang’s `.d`
files on the client to track which headers each compilation used. These reads
appear as `download outputs` in the profile even when the `.o` stays remote.
To get a local copy of the executable, add `--remote_download_outputs=toplevel`
to the build of `//:clickhouse`. The output is a Linux ELF even when the client is macOS.

ClickHouse uses hermetic-llvm’s `headers_directory` for complete public header
directories in Boost, AWS, ICU, and other dependencies. Boost also groups
directories containing its published textual headers. Bazel can reuse a
directory’s Merkle digest across compiles. DWYU and module maps still see the
individual public, private, and textual header declarations. Rules that enumerate
`CcInfo.compilation_context.headers` see the staged directories. Use
`--strict_system_includes` when such a rule needs the individual files.

Cargo stages complete directories from its pinned Rust repositories, including
Wasmtime and pcodec. Its command line still names each original source file.
cbindgen stages the vendored crates and Delta Kernel by directory, while keeping
files it names separately, such as Delta Kernel's `ffi/Cargo.toml`, as individual inputs.

MongoDB and LLVM stage complete directories of public and textual
headers. Boost, AWS, and simdjson can also stage complete directories
containing their own private headers; direct header visibility stays
unchanged. gRPC keeps its implementation headers out of consumer
compile actions. Its secure and insecure libraries share the public C
headers. `//src:dbms__compile_0` no longer exports the Prometheus
grammar.
Compared with `a1b2c5e82`, the complete change gave these declared input
counts for local configured compile actions:

- `src/Functions/area.cpp` (x86_64, normal): 15,752 to 13,452 inputs.
- `src/Functions/area.cpp` (aarch64, normal): 15,693 to 13,393 inputs.

These x86-64 timing profiles predate the source directory changes.
They used a local Bazel client with `--jobs=2000`
and a warm repository cache. Each run set a fresh target `--action_env` cache
buster, kept the host action environment unchanged, and executed the final
`ClickHouseNativeLink` on BuildBuddy. The hashes identify the `//:clickhouse` executables;
the [CMake comparison](#binary-comparison) below uses separate builds.

| BuildBuddy timing profile | ClickHouse SHA-256 | Bytes |
| --- | --- | ---: |
| [`-c fastbuild`](https://app.buildbuddy.io/invocation/b5f2d5ff-b97e-423b-bec7-b32127aee8cf#timing) | `66bfdb7a22d08f7076721fa1cd68ee7c5167b4d8d6620965a0db72858280c522` | 2,175,634,528 |
| [`-c opt --features=thin_lto`](https://app.buildbuddy.io/invocation/841c82aa-2b17-4df6-890f-8ac960d94733#timing) | `79d90fd7278aa71960be5ebf109b1ee14927d068884a94458f42863a58308353` | 828,248,440 |

## Linux headers and link inputs

Bazel C and C++ rules use the headers selected by the [target platforms](bazel/BUILD.bazel):
glibc 2.27 and Linux 4.15.18 on x86-64, glibc 2.28 and Linux 4.19.325 on ARM64.
Native ARM64 CMake uses Linux 4.19.0 headers with ClickHouse backports. The
[archive patches](bazel/llvm.MODULE.bazel) retain the seccomp, CPU feature, and memory
advice definitions that ClickHouse uses. The LLVM runtime compile rules declare these glibc and Linux headers.

Both platforms patch glibc's `string.h`
so Clang uses the libc++ overloads of `memchr` and `strchr`, as it does in the native
CMake build. The glibc overloads declare these functions `noexcept`; selecting them
changed code in ClickHouse, Boost.Asio, and ICU.

Bazel no longer registers `@clickhouse_sysroot`. Neither build of `//:clickhouse` needs the full ClickHouse SDK. Upstream CMake retains its `contrib/sysroot` gitlink; the Bazel ARM64 build fetches four GCC files from that pinned revision.

The final link takes `Scrt1.o`, `crti.o` and `crtn.o` from `@llvm//runtimes/glibc:pie_startup_objects` (glibc 2.31 on x86-64, 2.28 on ARM64). [The selected link runtime](bazel/link_runtime/BUILD.bazel) supplies 12 glibc and loader files plus GCC’s `crtbeginS.o` and `crtendS.o`. Clang points `--sysroot` into that runtime.

| Target | Source of the remaining glibc and GCC link files | Files available to each Cargo action |
| --- | --- | ---: |
| x86-64 | [Ubuntu 20.04](bazel/link_runtime/ubuntu.bzl): `libc6`, `libc6-dev` and `libgcc-9-dev` | 25 |
| ARM64 | [Arm GNU 8.3-2019.03](bazel/link_runtime/arm.bzl) glibc 2.28; [four pinned GCC files](https://github.com/ClickHouse/sysroot/tree/6dd29c222896e54de24e7af1b191c92e4cf2e229/linux-aarch64/lib/gcc/aarch64-linux-gnu/8.3.0): `crtbegin.o`, `crtbeginS.o`, `crtend.o` and `libgcc.a`. `crtendS.o` is a copy of `crtend.o`. | 24 |

The seven [Cargo rules](bazel/rules/rust_archives.bzl) use the files above. The [Rust archive rule](bazel/rules/rust_archive.bzl) reads libm symbol names from `@glibc//:libm_abi`.

A [previous ARM64 build](https://app.buildbuddy.io/invocation/c8f333e4-b619-4fa2-b0d9-c4453e0f205e) replaced five glibc libraries and the loader with generated interfaces; its stripped ClickHouse matched native CMake. The builds below used the packaged libraries and loader and did not rerun that experiment.

## Binary comparison

At [aece10433](https://github.com/dzbarsky/clickhouse/commit/aece10433134e227ae47077c2e04ef43496a0b9d), both `-c opt` builds produced the same stripped ClickHouse SHA-256 and byte count as native CMake.

| Native Bazel build | Stripped ClickHouse SHA-256 | Bytes |
| --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/10a5d66b-96b4-41a7-a02a-a1bcbd7b4b04) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710693736 |
| [ARM64](https://app.buildbuddy.io/invocation/47d07839-ff5e-4172-90cb-da35a69b739a) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637316696 |

The [validation history](bazel/initial_migration/validation-history.md#5549bc23a) records the unstripped hashes and compiler checks for the previous comparison.

To check a change against these references without downloading either ELF, run:

```sh
bash bazel/initial_migration/run_stripped_hash.sh
CMAKE_REFERENCE_ARCH=aarch64 bash bazel/initial_migration/run_stripped_hash.sh
```

The script uses minimal remote downloads and compares the SHA-256 and byte
count in Bazel's Build Event Protocol (BEP) with the native CMake reference.
To keep the BEP for inspection, run the underlying commands directly:

```sh
bazel build --config=cmake_match --build_event_json_file=/tmp/clickhouse-x86.bep \
  //:clickhouse //bazel/initial_migration:clickhouse.normalized
python3 bazel/initial_migration/read_binary_metadata.py --arch x86_64 /tmp/clickhouse-x86.bep

bazel build --config=cmake_match_aarch64 --build_event_json_file=/tmp/clickhouse-arm.bep \
  //:clickhouse //bazel/initial_migration:clickhouse.normalized
python3 bazel/initial_migration/read_binary_metadata.py --arch aarch64 /tmp/clickhouse-arm.bep
```

The `cmake_match` configurations use the recorded CMake build metadata.
[`clickhouse.normalized`](bazel/initial_migration/normalized_clickhouse.bzl)
checks the ELF architecture, then runs `@llvm//tools:llvm-strip` with `--strip-all
--remove-section=.note.gnu.build-id`. The [metadata reader](bazel/initial_migration/read_binary_metadata.py)
requires successful build and target completions in one configuration. It
reports both remote SHA-256 values and byte counts and checks that the build
events agree with the remote blob URIs. It compares the normalized SHA-256
and byte count with the recorded CMake values: exit 0 means match, 1 means
mismatch, and 2 means invalid or missing metadata. This command does not rebuild CMake.

To compare the actual normalized files with `cmp` on x86-64, run on a Linux
x86-64 coordinator with BuildBuddy access to the recorded CMake executable:

```sh
bash bazel/initial_migration/run_stripped_hash.sh --compare-originals
```

For ARM64, set `CMAKE_REFERENCE_ELF` to the saved CMake executable, either
raw or gzip-compressed, on the machine running Bazel:

```sh
CMAKE_REFERENCE_ARCH=aarch64 CMAKE_REFERENCE_ELF=/path/to/clickhouse.cmake.gz \
  bash bazel/initial_migration/run_stripped_hash.sh --compare-originals
```

Before invoking Bazel, the script verifies that the uncompressed file matches
`clickhouse.cmake` from the [recorded native ARM64 CMake build](https://app.buildbuddy.io/invocation/12df76fc-3a68-46da-8af4-1e01c1dd4b4b):
907,067,440 bytes with SHA-256 `cd204de2504bbe77c5e2b0babce701943133374e77490f36ad8b679d18b56646`.
A Linux ARM64 executor checks the compiler, strips both executables, and runs
`cmp`. The coordinator downloads only the comparison result, `status.json`.

## Ordinary ClickHouse builds

Both builds used `--//bazel:cmake_metadata=false` at [babb29e66](https://github.com/dzbarsky/clickhouse/commit/babb29e66c3ca06418eef4e8c73f1529334ae1ab). Each native executor ran SQL and PRQL queries with the resulting `//:clickhouse` executable.

| Build | Raw executable SHA-256 | Bytes |
| --- | --- | ---: |
| [x86-64 `-c fastbuild`](https://app.buildbuddy.io/invocation/9594b53e-d090-4a1e-8a2c-7a62f3a0169b) | `66bfdb7a22d08f7076721fa1cd68ee7c5167b4d8d6620965a0db72858280c522` | 2175634528 |
| [ARM64 `-c opt`](https://app.buildbuddy.io/invocation/b78c9672-5b35-4189-91ab-adca9678194d) | `68ffac97099dbd03449efcb2a90043047dca0914657bd7a2dfe8fb19feb3f954` | 911410032 |

The x86-64 fastbuild matches the earlier x86-64 fastbuild when compared by SHA-256 and byte count. The earlier executable had SHA-256 `66bfdb7a22d08f7076721fa1cd68ee7c5167b4d8d6620965a0db72858280c522` and 2175634528 bytes. [Earlier fastbuild](https://app.buildbuddy.io/invocation/b5f2d5ff-b97e-423b-bec7-b32127aee8cf)

## Rebuild a native CMake reference

The ClickHouse source baseline is [`6bb427c9a`](https://github.com/ClickHouse/ClickHouse/commit/6bb427c9a81459c2b9e7369ef6ed8a6e8680b0da).
A new source revision or embedded Git/build metadata can change the reference
hash. The [x86-64](https://app.buildbuddy.io/invocation/10ea5019-d0e8-4f22-bb88-ef81fce6ac47)
and [ARM64](https://app.buildbuddy.io/invocation/7a855e69-ff30-4b33-92b0-7c61ed6b6517)
reference comparisons ran `cmp` on the actual normalized files.

On a Linux x86-64 host with BuildBuddy configured, the script initializes
submodules, installs the selected compiler, and runs Release CMake and Ninja:

```sh
CMAKE_REFERENCE_ARCH=x86_64 bash bazel/initial_migration/run_cmake_reference.sh
ref=build_bazel_reference_x86_64
build_bazel_reference_tools/llvm/bin/llvm-strip --strip-all --remove-section=.note.gnu.build-id \
  -o "$ref/clickhouse.normalized" "$ref/programs/clickhouse"
sha256sum "$ref/clickhouse.normalized"
```

On a BuildBuddy hosted runner, set `CMAKE_REFERENCE_ARCHIVE_BINARY=1` to
upload the raw x86-64 CMake executable after a successful build.

For ARM64, run the same entry point on a Linux BuildBuddy runner:

```sh
CMAKE_REFERENCE_ARCH=aarch64 bash bazel/initial_migration/run_cmake_reference.sh
```

The runner prepares the source and tool archives. `//bazel:cmake_reference_aarch64`
runs CMake and Ninja on a Linux ARM64 executor and checks that Clang, Rust,
TableGen and ClickHouse are ARM64 executables. The runner downloads `status.json`
and `cmake-reference-summary.json`. A failed CMake action returns a nonzero
exit code and writes diagnostics to the BuildBuddy action log.

When replacing the ARM64 reference, first run
`bash bazel/initial_migration/run_native_reference.sh --migration-evidence`
to download the generated files. Use
[`refresh_cmake_metadata.py`](bazel/initial_migration/refresh_cmake_metadata.py)
with `--arch` and `--build-dir` to update the Bazel metadata from the CMake
output. Add `--full-reference` for ARM64; `--check` verifies without writing.

## Check C++ dependencies

`--config=dwyu` runs the development-only [Depend On What You Use aspect](bazel/rules/dwyu.bzl).
Request the libraries you changed; the custom `//:clickhouse` link does not
itself compile C++. For example:

```sh
bazel build --config=cmake_match --config=dwyu //base/readpassphrase @clickhouse_libuv//:_uv
bazel build --config=cmake_match_aarch64 --config=dwyu //base/readpassphrase @clickhouse_libuv//:_uv
```

Check the include sites: public headers need `deps`; dependencies used only
by the implementation belong in `implementation_deps`. A dependency may
still be required for linking. If preprocessing fails, an owned file is not
entered by Clang, or a link dependency cannot be verified, that target has
not passed. For the configured dependencies of the binary, use a new output
directory per invocation. To include additional tests or libraries, repeat
`--seed //:clickhouse --seed <label>`; explicit seeds replace the default.

```sh
python3 bazel/initial_migration/audit_full_dwyu.py --config cmake_match --out /tmp/clickhouse-dwyu-x86
python3 bazel/initial_migration/audit_full_dwyu.py --config cmake_match_aarch64 --out /tmp/clickhouse-dwyu-arm
```

This revision produced verified DWYU reports for 3 x86-64 targets and 4 ARM64 targets. Across recorded revisions, DWYU has graded 432 of 627 x86-64 targets and 421 of 612 ARM64 targets. The [validation history](bazel/initial_migration/validation-history.md#5549bc23a) lists this revision’s results.
