#!/usr/bin/env bash

set -e

# std is required for sanitizers builds
# and we need to match toolchain version for std (to vendor proper dependencies)
TOOLCHAIN=nightly-2024-12-01
function cargo() { rustup run "$TOOLCHAIN" cargo "$@"; }
function rustc() { rustup run "$TOOLCHAIN" rustc "$@"; }
rustup component add --toolchain "$TOOLCHAIN" rust-src

CH_TOP_DIR=$(git rev-parse --show-toplevel)

cd "$CH_TOP_DIR/rust/workspace" || exit 1
# (Re)-generate the Cargo.lock file
cargo generate-lockfile

#
# Generate the vendor dir:
#

# Clean the vendor repo
rm -rf "${CH_TOP_DIR:?}"/contrib/rust_vendor/*

cd "$CH_TOP_DIR"/rust/workspace || exit 1
cargo vendor --no-delete --locked --versioned-dirs --manifest-path Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

#
# Handle extra dependencies that is now outside of workspace:
#

# delta-lake
cd "$CH_TOP_DIR"/contrib/delta-kernel-rs || exit 1
cargo vendor --no-delete --locked --versioned-dirs --manifest-path Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

# chdig
cd "$CH_TOP_DIR"/contrib/chdig || exit 1
cargo vendor --no-delete --locked --versioned-dirs --manifest-path Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

# Just in case
cd "$CH_TOP_DIR"/rust/workspace

# Standard library deps
RUSTC_ROOT=$(rustc --print=sysroot)
cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor
cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

# Now let's remove windows crates - we don't support windows.
#
# Note, there is also cargo vendor-filterer that supports --platform, but it cannot be used due to:
# - it does not support --no-delete (and refuses to run on non-empty directory)
# - missing --locked
#
# Refs: https://github.com/rust-lang/cargo/issues/7058
rm -fr "$CH_TOP_DIR"/contrib/rust_vendor/windows*/lib/*.a
rm -fr "$CH_TOP_DIR"/contrib/rust_vendor/winapi*/lib/*.a
rm -fr "$CH_TOP_DIR"/contrib/rust_vendor/winapi*/lib/*.lib
rm -fr "$CH_TOP_DIR"/contrib/rust_vendor/windows*/lib/*.lib

echo "*"
echo "* Do not forget to check contrib/corrosion-cmake/config.toml.in"
echo "* You need to make sure that it contains everything that is printed under"
echo "      'To use vendored sources, add this to your .cargo/config.toml for this project:'"
echo "*"
