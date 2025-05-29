#!/usr/bin/env bash

set -e

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

# Just in case
cd "$CH_TOP_DIR"/rust/workspace

# Standard library deps
RUSTC_ROOT=$(rustc --print=sysroot)
cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/std/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor
cargo vendor --no-delete --locked --versioned-dirs --manifest-path "$RUSTC_ROOT"/lib/rustlib/src/rust/library/test/Cargo.toml "$CH_TOP_DIR"/contrib/rust_vendor

echo "*"
echo "* Do not forget to check contrib/corrosion-cmake/config.toml.in"
echo "* You need to make sure that it contains everything that is printed under"
echo "      'To use vendored sources, add this to your .cargo/config.toml for this project:'"
echo "*"
