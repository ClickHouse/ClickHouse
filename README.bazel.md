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

Remote builds use `--remote_download_outputs=minimal`. To get a local copy
of the executable, add `--remote_download_outputs=toplevel` to the build
of `//:clickhouse`. The output is a Linux ELF even when the client is macOS.

## Binary comparison

Bazel source `4e214b47a` built all 57 requested targets on each CPU. The normalized ClickHouse ELF matched the SHA-256 and size from native CMake:

| Native Bazel build | Normalized SHA-256 | Bytes |
| --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/50d792e9-3a3b-4916-8844-1a1d563162ce) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710,693,736 |
| [ARM64](https://app.buildbuddy.io/invocation/88071a32-86d0-406b-b5d4-6dcb6ed6702e) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637,316,696 |

Both the raw ClickHouse ELF and `//src:unit_tests_dbms` also matched the [previous published build](https://github.com/dzbarsky/clickhouse/commit/a67536970816be10181b3054b25178ac3d0f026c). Each CPU passed 38 GoogleTests and six SQL cases. The comparison used BuildBuddy output metadata without downloading the executables.

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
checks the compiler and ELF architecture, then runs `llvm-strip --strip-all
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

The `4e214b47a` builds did not run DWYU. The two native Bazel builds passed, then their parent workflows failed while reading their output. All 784 declared production targets on each CPU remain unchecked at this revision. See the [validation history](bazel/initial_migration/validation-history.md#4e214b47a) for the parent invocations and earlier findings.
