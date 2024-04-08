As we have multiple projects we use a workspace to manage them (it's way simpler and leads to less issues). In order
to vendor all the dependencies we need to store both the registry and the packages themselves.

* First step: (Re)-generate the Cargo.lock file (run under `workspace/`)

```bash
$ cargo generate-lockfile
```

* Generate the local registry:

To install the tool if you don't already have it:
```bash
cargo install --version 0.2.6 cargo-local-registry
```

Now run:

```bash
cargo local-registry --git --sync Cargo.lock ../../contrib/rust_vendor
cargo vendor --no-delete --locked ../../contrib/rust_vendor
```

Note that we use both commands to vendor both registry and crates. No idea why both are necessary.
