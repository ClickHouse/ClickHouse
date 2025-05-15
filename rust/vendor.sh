#!/usr/bin/env bash

set -ex

export CH_TOP_DIR=$(git rev-parse --show-toplevel)
# The `rustc --print=sysroot` part includes `std` dependencies, required to
# build with sanitizer flags. It must be kept in sync with the rustc version
# used in CI.
export RUSTC_ROOT=$(rustc --print=sysroot)
# TODO: Currently the following are built outside the workspace
export DELTA_LAKE_DIR="$CH_TOP_DIR"/contrib/delta-kernel-rs
export CHDIG_DIR="$CH_TOP_DIR"/contrib/chdig

# Clean the vendor repo
rm -rf "$CH_TOP_DIR"/contrib/rust_vendor/*

cd "$CH_TOP_DIR"/rust/workspace
cargo local-registry --git --sync Cargo.lock "$CH_TOP_DIR"/contrib/rust_vendor

# Now handle out side of workspace crates
cd "$DELTA_LAKE_DIR"
cargo local-registry --no-delete --git --sync "$DELTA_LAKE_DIR/Cargo.lock" "$CH_TOP_DIR"/contrib/rust_vendor
cd "$CHDIG_DIR"
cargo local-registry --no-delete --git --sync "$CHDIG_DIR/Cargo.lock" "$CH_TOP_DIR"/contrib/rust_vendor

# Standard library deps
cp "$RUSTC_ROOT"/lib/rustlib/src/rust/library/Cargo.lock "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/
cargo local-registry --no-delete --git --sync "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/Cargo.lock "$CH_TOP_DIR"/contrib/rust_vendor
cp "$RUSTC_ROOT"/lib/rustlib/src/rust/library/Cargo.lock "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/
cargo local-registry --no-delete --git --sync "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.lock "$CH_TOP_DIR"/contrib/rust_vendor

# Now we vendor the modules themselves
cd "$CH_TOP_DIR"/rust/workspace
cargo vendor --no-delete --locked "$CH_TOP_DIR"/contrib/rust_vendor
cd "$DELTA_LAKE_DIR"
cargo vendor --no-delete --locked "$CH_TOP_DIR"/contrib/rust_vendor
cd "$CHDIG_DIR"
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
