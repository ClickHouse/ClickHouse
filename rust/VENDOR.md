As we have multiple projects we use a workspace to manage them (it's way simpler and leads to less issues). In order
to vendor all the dependencies we need to store both the registry and the packages themselves.

Note that this includes the exact `std` dependencies for the rustc version used in CI (currently nightly-2024-12-01),
so you need to install `rustup component add rust-src` for the specific version.

* First step: (Re)-generate the Cargo.lock file (run under `workspace/`).

```bash
cargo generate-lockfile
```

* Generate the local registry:

Note that we use both commands to vendor both registry and crates. No idea why both are necessary.

  * First we need to install the tool if you don't already have it:

    ```bash
    cargo install --version 0.2.7 cargo-local-registry
    ```

  * Now add local packages with `vendor.sh`
