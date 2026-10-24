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

Bazel source `234b7aab0` built all 51 requested targets on each CPU. Both builds linked `//:clickhouse` and `//src:unit_tests_dbms`; neither ran the unit tests. The normalized ELF's SHA-256 and byte count matched the recorded native CMake reference on each CPU. Separate compile checks built the five WebUI objects after the generated gzip inputs were added to `//src:dbms_embedded`.

| Native Bazel build | Normalized SHA-256 (matches CMake) | Bytes |
| --- | --- | ---: |
| [x86-64](https://app.buildbuddy.io/invocation/4e5a002f-86ec-4495-a0f5-8277177058dc) | `ccf17074f256c8cbc48d64d0aea0cf7b6a178b7412f16f337ba240aa130f1585` | 710693736 |
| [ARM64](https://app.buildbuddy.io/invocation/b4894636-e1ad-4524-8d05-c18b179abcc7) | `a29d49408309ba5719e5ab5d7c3eac2334584f91e2bcbc30d9df7feb55284d03` | 637316696 |

To check a change against these references without downloading either ELF:

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
against the recorded CMake hash: exit 0 means match, 1 means mismatch, and
2 means invalid or missing metadata. This command does not rebuild CMake.

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

For ARM64, prepare the source and tool archives on a Linux BuildBuddy runner.
`//bazel:cmake_reference_aarch64` runs CMake on an ARM64 executor and checks
that Clang, Rust, TableGen and ClickHouse are ARM64 executables. Download
this target's outputs to inspect the CMake exit status and measured hashes:

```sh
python3 -B bazel/initial_migration/prepare_native_reference.py
bazel build --config=cmake_match_aarch64 --remote_download_outputs=toplevel //bazel:cmake_reference_aarch64
ref=bazel-bin/bazel/cmake_reference_aarch64
python3 -c 'import json,sys; s=json.load(open(sys.argv[1])); print(s); sys.exit(s["status"] or (s["phase"] != "built"))' "$ref/status.json" &&
  cat "$ref/cmake-reference-summary.json"
```

The ARM64 rule saves diagnostics even on failure: require `status: 0` and
`phase: "built"` in `status.json`. When replacing a reference, use
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

**DWYU is pending for `234b7aab0`.** The binary comparison above does not evaluate dependency declarations. See the [validation history](bazel/initial_migration/validation-history.md) for earlier audit results.
