As we have multiple projects we use a workspace to manage them (it's way simpler and leads to less issues). In order
to vendor all the dependencies we need to store both the registry and the packages themselves.

Note that this includes the exact `std` dependencies for the rustc version used in CI (currently nightly-2024-12-01),
so you need to install `rustup component add rust-src` for the specific version.

* First step: (Re)-generate the Cargo.lock file (run under `workspace/`).

```bash
cargo generate-lockfile
```

* Generate the vendor dir:

```bash
export CH_TOP_DIR=$(git rev-parse --show-toplevel)
export RUSTC_ROOT=$(rustc --print=sysroot)
# Currently delta-lake is built outside the workspace (TODO)
export DELTA_LAKE_DIR="$CH_TOP_DIR"/contrib/delta-kernel-rs

# cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

# Clean the vendor repo
rm -rf "$CH_TOP_DIR"/contrib/rust_vendor/*

cd "$CH_TOP_DIR"/rust/workspace
cargo cargo vendor --no-delete --locked --versioned-dirs --manifest-path Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

# Now handle delta-lake
cd "$DELTA_LAKE_DIR"
cargo cargo vendor --no-delete --locked --versioned-dirs --manifest-path Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

# Standard library deps
cargo cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor
cargo cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

cd "$CH_TOP_DIR"/rust/workspace
```

The `rustc --print=sysroot` part includes `std` dependencies, required to build with sanitizer flags. It must be kept
in sync with the rustc version used in CI.
