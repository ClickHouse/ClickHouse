# update-contrib: libraries built through the Rust workspace

Read this only when `rust/workspace/<lib>/` exists for the library being bumped. The integration points are:
- `rust/workspace/<lib>/Cargo.toml` — dependency path, crate name, enabled features
- `rust/workspace/<lib>/CMakeLists.txt` — generated headers, copied headers, CMake target names
- `rust/workspace/Cargo.lock`
- `contrib/rust_vendor/` — a submodule pointing at `github.com/ClickHouse/rust_vendor`

Updating such a library often means regenerating `rust/workspace/Cargo.lock`, re-vendoring crates with
`rust/vendor.sh`, and verifying the Rust/C++ bridge build rather than editing `contrib/<lib>-cmake/`.

## Gather information

```bash
ls rust/workspace/${LIB}/Cargo.toml rust/workspace/${LIB}/CMakeLists.txt 2>/dev/null
grep -R "contrib/${LIB}\|${LIB}" rust/workspace/ --include='Cargo.toml' --include='CMakeLists.txt'
```

Inspect `Cargo.toml` (path, crate name, features), `CMakeLists.txt` (generated/copied headers, target names) and
`Cargo.lock` (what will need regenerating).

## Diff the integration surface

Diff the surface ClickHouse uses rather than only the whole upstream tree: `crates/c-api/`, public headers copied
into the build tree, Cargo feature lists and crate names used by `rust/workspace/<lib>/Cargo.toml`. If the public
C/C++ API changes are additive only, the update may be low-risk even when the full upstream diff is huge.

## Update build integration

1. Check that `rust/workspace/<lib>/Cargo.toml` still points to the correct path/package and that the enabled
   features are still valid.
2. Check that `rust/workspace/<lib>/CMakeLists.txt` still matches the upstream public headers and feature flags.
3. If headers are copied into the build directory at configure time, changes to submodule headers may require
   re-running CMake or manually refreshing the copied headers for incremental verification.
4. Regenerate `rust/workspace/Cargo.lock` if dependency resolution changed.
5. Re-vendor crates with `rust/vendor.sh`. It re-vendors the whole workspace, not just the target library, and can
   produce hundreds of file changes under `contrib/rust_vendor/`. Those changes must be committed inside the
   `rust_vendor` submodule on a branch named `bump-${LIB}-${VERSION}`; then bump the submodule pointer in ClickHouse.
   Push the `rust_vendor` branch only after the user explicitly confirms.

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
git add "contrib/$LIB" rust/workspace/Cargo.lock contrib/rust_vendor
```

`contrib/rust_vendor` stages the bumped submodule pointer after its own branch has been committed; the vendored files
themselves are not tracked in the ClickHouse repo. Include `rust/workspace/Cargo.lock` and the `rust_vendor` pointer
in the final ClickHouse commit.
