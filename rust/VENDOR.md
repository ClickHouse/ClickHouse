As we have multiple projects we use a workspace to manage them (it's way
simpler and leads to less issues). In order to vendor all the dependencies we
need to store both the registry and the packages themselves.

Note that this includes the exact `std` dependencies for the rustc (required to
build with sanitizers flags) version used in CI (currently nightly-2024-12-01),
so you need to install `rustup component add rust-src` for the specific
version.

To re-generate just use `vendor.sh`
