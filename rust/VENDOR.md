Each included library is treated independently (it'd be simpler to have them in a common rust project) and for each we need to save both the dependencies and the registry (so it's possible to build without registry).

To do this we use the `cargo-local-registry` utility (this requires a modern toolchain)

```bash
cargo install --version 0.2.6 cargo-local-registry
```

From this directory, save each library independently:

```bash
cargo local-registry --git --sync prql/Cargo.lock ../contrib/rust_vendor/prql
cargo local-registry --git --sync skim/Cargo.lock ../contrib/rust_vendor/skim
# For skim we need to run an additional vendor to get the github patch. Awful stuff
cd skim
cargo vendor --no-delete --locked ../../contrib/rust_vendor/skim
```
