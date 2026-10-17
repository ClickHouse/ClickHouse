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

The `aarch64` configuration supports cross-compilation. ARM64 execution is
outside the current validation run.

Dependency ownership, generation, and action-audit details are in
[`bazel/README.md`](bazel/README.md).

## Binary comparison

To replay the CMake reference's embedded compiler paths, versions, flags,
and CMake version, and omit the extra `BUILD_SYSTEM` row, use:

```sh
bazel build --config=x86_64 --//bazel:cmake_metadata //:clickhouse
```

This opt-in changes the reported metadata only. It uses the values recorded
by `bazel/generate_build.py` from the configured CMake reference, so that
reference must match the CMake build being compared. It does not change the
selected compiler, optimization, dependencies, or link behavior. Without the
flag, `system.build_options` reports the actual Bazel toolchain and flags.
The metadata generation has been executed in both modes; with the flag, its
output matches the configured CMake metadata source byte for byte.

The final Linux x86-64 CMake and Bazel builds are being validated. Their
SHA-256 values have not yet been measured, and binary equality is unverified.
The completed comparison will record both executable hashes, the source
revision, compiler, build options, and reproduction commands here.

Hashes cover the linked executable bytes, before any extra stripping or
packaging step. Action-graph parity alone does not establish equal hashes.
