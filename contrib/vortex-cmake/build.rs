//! Generates `include/vortex_ffi.h` from `src/lib.rs` and fails the build when the committed
//! header is not the one it generates.
//!
//! The two halves of this FFI are separate translation units linked by symbol name, so a signature
//! changed on one side only is silent undefined behaviour rather than a compile error. Generating
//! the header rules that out; checking it here rules out forgetting to regenerate:
//!
//!     VORTEX_FFI_HEADER_UPDATE=1 cargo check -p _ch_rust_vortex

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

const UPDATE_VAR: &str = "VORTEX_FFI_HEADER_UPDATE";

#[derive(Deserialize)]
struct Lock {
    package: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    version: String,
}

fn main() {
    emit_vortex_version();

    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set");
    let crate_dir = Path::new(&crate_dir);
    let header_path = crate_dir.join("include/vortex_ffi.h");

    println!("cargo::rerun-if-changed=src/lib.rs");
    println!("cargo::rerun-if-changed=cbindgen.toml");
    // An input as well: editing the header by hand has to bring the check back.
    println!("cargo::rerun-if-changed=include/vortex_ffi.h");
    println!("cargo::rerun-if-env-changed={UPDATE_VAR}");

    let config_path = crate_dir.join("cbindgen.toml");
    let config = cbindgen::Config::from_file(&config_path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", config_path.display()));

    // The file rather than the crate: `with_crate` shells out to `cargo metadata`, which does not
    // inherit the `--config` that points cargo at the vendored sources. Everything the header
    // exposes is defined here anyway.
    let mut generated = Vec::new();
    cbindgen::Builder::new()
        .with_config(config)
        .with_src(crate_dir.join("src/lib.rs"))
        .generate()
        .unwrap_or_else(|error| panic!("cannot generate the header from src/lib.rs: {error}"))
        .write(&mut generated);

    // A missing header counts as out of date.
    if fs::read(&header_path).unwrap_or_default() == generated {
        return;
    }

    if env::var_os(UPDATE_VAR).is_some() {
        fs::write(&header_path, &generated)
            .unwrap_or_else(|error| panic!("cannot write {}: {error}", header_path.display()));
        return;
    }

    // Not a panic: nothing here is broken, this is a message to whoever changed `src/lib.rs`.
    println!(
        "cargo::error=include/vortex_ffi.h is not what src/lib.rs generates. \
         Run `{UPDATE_VAR}=1 cargo check -p _ch_rust_vortex` and commit the result."
    );
}

fn emit_vortex_version() {
    let lock_path = workspace_dir().join("Cargo.lock");
    println!("cargo::rerun-if-changed={}", lock_path.display());

    let lock = fs::read_to_string(&lock_path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", lock_path.display()));
    let lock: Lock = toml::from_str(&lock)
        .unwrap_or_else(|error| panic!("cannot parse {}: {error}", lock_path.display()));

    let versions: Vec<&str> = lock
        .package
        .iter()
        .filter(|package| package.name == "vortex")
        .map(|package| package.version.as_str())
        .collect();
    let [version] = versions[..] else {
        panic!("expected one version of `vortex` in {}, found {versions:?}", lock_path.display());
    };
    println!("cargo::rustc-env=VORTEX_CRATE_VERSION={version}");
}

fn workspace_dir() -> PathBuf {
    let cargo = env::var_os("CARGO").expect("CARGO should be set");
    let output = Command::new(cargo)
        .args(["locate-project", "--workspace", "--message-format", "plain"])
        .output()
        .expect("cannot run `cargo locate-project`");
    assert!(
        output.status.success(),
        "`cargo locate-project` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let manifest = String::from_utf8(output.stdout).expect("the path should be UTF-8");
    Path::new(manifest.trim()).parent().expect("the path should have a parent").to_owned()
}
