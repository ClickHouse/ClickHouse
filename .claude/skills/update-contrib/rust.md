# update-contrib: libraries built by cargo

Read this when the library is built by cargo. Two layouts exist:
- `contrib/<lib>-cmake/Cargo.toml` exists (`wasmtime`, `pcodec`): a member of the workspace in
  `contrib/rust-contrib-workspace/`. `Cargo.toml` (dependency path, crate name, enabled features), `CMakeLists.txt`
  (generated headers, copied headers, CMake target names), plus the shared `contrib/rust-contrib-workspace/Cargo.lock`
  and `contrib/rust_vendor/` (a submodule pointing at `github.com/ClickHouse/rust_vendor`).
- `contrib/<lib>-cmake/CMakeLists.txt` drives cargo itself (`chdig`, `delta-kernel-rs`): the crate lives in the
  submodule, the `*-cmake/` file runs the build and exports the alias; there is no `contrib/<lib>-cmake/Cargo.toml`.

Both export `ch_rust::<name>` aliases, not `ch_contrib::*`; the name may differ from the contrib directory
(`ch_rust::delta_kernel_rs` for `contrib/delta-kernel-rs`). Take the names from the integration file (SKILL.md step 1).

Decide the layout first — the steps below differ:

```bash
[ -f "contrib/${LIB}-cmake/Cargo.toml" ] && echo "layout: workspace" || echo "layout: cmake-driven cargo"
```

## Layout A: a member of `contrib/rust-contrib-workspace/` (`wasmtime`, `pcodec`)

Gather information:

```bash
ls contrib/${LIB}-cmake/Cargo.toml contrib/${LIB}-cmake/CMakeLists.txt
grep -R "contrib/${LIB}\|${LIB}" contrib/${LIB}-cmake/ contrib/rust-contrib-workspace/ --include='Cargo.toml' --include='CMakeLists.txt'
```

Inspect `Cargo.toml` (path, crate name, features), `CMakeLists.txt` (generated/copied headers, target names) and
the shared `contrib/rust-contrib-workspace/Cargo.lock` (what will need regenerating). Diff the surface ClickHouse uses rather than
only the whole upstream tree: `crates/c-api/`, public headers copied into the build tree, Cargo feature lists and
crate names used by `contrib/<lib>-cmake/Cargo.toml`. If the public C/C++ API changes are additive only, the update
may be low-risk even when the full upstream diff is huge.

Update build integration:

1. Check that `contrib/<lib>-cmake/Cargo.toml` still points to the correct path/package and that the enabled
   features are still valid.
2. Check that `contrib/<lib>-cmake/CMakeLists.txt` still matches the upstream public headers and feature flags.
3. If headers are copied into the build directory at configure time, changes to submodule headers may require
   re-running CMake or manually refreshing the copied headers for incremental verification.
4. Regenerate `contrib/rust-contrib-workspace/Cargo.lock` if dependency resolution changed.
5. Re-vendor crates (see "Vendoring" below).

## Layout B: `contrib/<lib>-cmake/CMakeLists.txt` drives cargo (`chdig`, `delta-kernel-rs`)

There is no `contrib/<lib>-cmake/Cargo.toml` and the library is not a member of the workspace: the crate, its `Cargo.toml`
and its own `Cargo.lock` live inside the submodule, and the `*-cmake/` file calls `clickhouse_import_crate` with the
manifest path and the enabled features, sets cargo environment (`corrosion_set_env_vars`: OpenSSL paths, `LDFLAGS`)
and exports the `ch_rust::*` alias.

Gather information:

```bash
grep -n "clickhouse_import_crate\|FEATURES\|MANIFEST_PATH\|corrosion_set_env_vars\|add_library" contrib/${LIB}-cmake/CMakeLists.txt
ls contrib/${LIB}/Cargo.toml contrib/${LIB}/Cargo.lock contrib/${LIB}/ffi/Cargo.toml 2>/dev/null
```

Diff between old and new pin: the manifest that `MANIFEST_PATH` names (crate name, feature names — a removed or
renamed feature breaks `clickhouse_import_crate`), the FFI surface ClickHouse includes (`delta_kernel_ffi.hpp` is
generated at build time; check the C++ callers under `src/`), and the submodule's `Cargo.lock` (new crates need
vendoring, build-script crates such as `openssl-sys`/`aws-lc-sys` may need new environment in the `*-cmake/` file).

Update build integration:

1. Fix the `FEATURES` list and `MANIFEST_PATH` in `contrib/<lib>-cmake/CMakeLists.txt` if upstream changed them.
2. Do not edit or regenerate the submodule's `Cargo.lock`; the new pin brings its own and vendoring runs `--locked`.
3. Re-vendor crates (see "Vendoring" below).

## Vendoring (both layouts)

`contrib/rust-contrib-workspace/vendor.sh` re-vendors everything - the workspace, `contrib/delta-kernel-rs`, `contrib/chdig` and the standard
library sources - into `contrib/rust_vendor/`, a submodule pointing at `github.com/ClickHouse/rust_vendor`, and can
produce hundreds of file changes there. Those changes must be committed inside the `rust_vendor` submodule on a
branch named `bump-${LIB}-${VERSION}`; then bump the submodule pointer in ClickHouse. Push the `rust_vendor` branch
only after the user explicitly confirms (pipeline mode: a missing `rust_vendor` branch is a `BLOCKED.md` reason).

## Host limits seen on the pipeline's build host

- A fork-preparation run may only be able to validate with `cargo check -p <ffi crate>` (e.g. `delta_kernel_ffi`)
  when `contrib/rust_vendor` is stale against the new `Cargo.lock`; say in the report that ninja was not run.
- The host has no `pkg-config`/OpenSSL development files: crates whose default features pull `native-tls`/`openssl-sys`
  fail `cargo check` there; check with the feature set the `*-cmake/` file enables (it passes OpenSSL paths through
  `corrosion_set_env_vars`) rather than cargo's defaults.
- After a fresh vendor, the `crc-fast` build script failed to link (`-lgcc_s`/`-lutil` missing from the aarch64
  sysroot). Sysroot gaps are fixed in `ClickHouse/sysroot`, not with link flags in `contrib/*-cmake`; until then it is
  a `BLOCKED.md` reason, not something to work around.
- A crate's `rust-version` above the pinned toolchain (`Rust_TOOLCHAIN` in `contrib/corrosion-cmake/CMakeLists.txt`)
  means a toolchain bump (CI images too) — a human decision; stop and report it.

## Build

Discover the actual target names before building — do not guess:

```bash
ninja -C "$BUILD_DIR" -t targets | grep -i "$LIB"
```

The cargo target may be `_cargo-build__ch_rust_<lib>` rather than `_ch_rust_<lib>`; the final binary target is
`clickhouse`. If a failure mentions a copied upstream header, check whether a new API addition lacks its feature
guard in the C++ wrapper header, whether the C and C++ headers disagree on which features gate a declaration, or
whether the build directory still holds stale copied headers.

## Commit

Stage all integration artifacts together:

```bash
git add "contrib/$LIB" contrib/rust_vendor                        # both layouts
git add contrib/rust-contrib-workspace/Cargo.lock "contrib/${LIB}-cmake"   # layout A
git add "contrib/${LIB}-cmake/CMakeLists.txt"                     # layout B, when features/env changed
```

`contrib/rust_vendor` stages the bumped submodule pointer after its own branch has been committed; the vendored files
themselves are not tracked in the ClickHouse repo.
