As we have multiple projects we use a workspace to manage them (it's way simpler and leads to less issues). In order
to vendor all the dependencies we need to store both the registry and the packages themselves.

Note that this includes the exact `std` dependencies for the rustc version used in CI (currently nightly-2024-04-01),
so you need to install `rustup component add rust-src` for the specific version.

* First step: (Re)-generate the Cargo.lock file (run under `workspace/`).

```bash
cargo generate-lockfile
```

* Generate the local registry:

Note that we use both commands to vendor both registry and crates. No idea why both are necessary.

  * First we need to install the tool if you don't already have it:
```bash
cargo install --version 0.2.6 cargo-local-registry
```

  * Now add the local packages:

```bash
export CH_TOP_DIR=$(git rev-parse --show-toplevel)
export RUSTC_ROOT=$(rustc --print=sysroot)

cd "$CH_TOP_DIR"/rust/workspace

cargo local-registry --git --sync Cargo.lock "$CH_TOP_DIR"/contrib/rust_vendor
cp "$RUSTC_ROOT"/lib/rustlib/src/rust/Cargo.lock "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/
cargo local-registry --no-delete --git --sync "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/Cargo.lock "$CH_TOP_DIR"/contrib/rust_vendor
cp "$RUSTC_ROOT"/lib/rustlib/src/rust/Cargo.lock "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/
cargo local-registry --no-delete --git --sync "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.lock "$CH_TOP_DIR"/contrib/rust_vendor

cargo vendor --no-delete --locked "$CH_TOP_DIR"/contrib/rust_vendor
cd "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/
cargo vendor --no-delete "$CH_TOP_DIR"/contrib/rust_vendor
cd "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/
cargo vendor --no-delete "$CH_TOP_DIR"/contrib/rust_vendor

# Remove windows only dependencies (which are really heavy and we don't want in the repo)
rm -rf "$CH_TOP_DIR"/contrib/rust_vendor/winapi* "$CH_TOP_DIR"/contrib/rust_vendor/windows*

# Cleanup the lock files we copied
rm "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/Cargo.lock "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.lock
cd "$CH_TOP_DIR"/rust/workspace
```

The `rustc --print=sysroot` part includes `std` dependencies, required to build with sanitizer flags. It must be kept
in sync with the rustc version used in CI.
