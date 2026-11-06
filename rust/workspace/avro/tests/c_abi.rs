//! Compiles `tests/c_abi/smoke.c` as C and as C++ against `include/avro_rust.h`,
//! links it with the static library this package builds, and runs it. Skipped
//! (loudly) when no C compiler is available, unless AVRO_REQUIRE_FIXTURES is set.

use std::path::{Path, PathBuf};
use std::process::Command;

/// `env` (`CC` or `CXX`) names the compiler when it is set, as it does for the `cc` crate: the CI
/// image has only the versioned `clang-N`, which none of the default names find.
fn find_compiler(env: &str, names: &[&str]) -> Option<String> {
    if let Some(c) = std::env::var(env).ok().filter(|c| !c.is_empty()) {
        return Some(c);
    }
    let dirs = ["/opt/homebrew/opt/llvm/bin", "/usr/bin", "/usr/local/bin"];
    for n in names {
        for d in dirs {
            let p = Path::new(d).join(n);
            if p.exists() {
                return Some(p.to_string_lossy().into_owned());
            }
        }
    }
    None
}

/// Builds the static library with the same cargo, profile and target directory as this test and
/// returns its path. (`cargo test` itself only builds the rlib.)
fn static_lib() -> Option<PathBuf> {
    // target/<profile>/deps/<test exe>
    let exe = std::env::current_exe().ok()?;
    let profile_dir = exe.parent()?.parent()?;
    let target_dir = profile_dir.parent()?;
    let mut cmd = Command::new(std::env::var_os("CARGO")?);
    cmd.args(["build", "--lib", "--offline", "--manifest-path"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .arg("--target-dir")
        .arg(target_dir);
    if profile_dir.file_name()? == "release" {
        cmd.arg("--release");
    }
    assert!(
        cmd.status().ok()?.success(),
        "building the static library failed"
    );
    let lib = profile_dir.join("lib_ch_rust_avro.a");
    lib.exists().then_some(lib)
}

#[test]
fn c_and_cpp_programs_link_and_run_against_the_static_library() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (Some(cc), Some(cxx), Some(lib)) = (
        find_compiler("CC", &["clang", "cc", "gcc"]),
        find_compiler("CXX", &["clang++", "c++", "g++"]),
        static_lib(),
    ) else {
        assert!(
            std::env::var_os("AVRO_REQUIRE_FIXTURES").is_none(),
            "no C compiler or no static library"
        );
        eprintln!("SKIPPED: no C compiler or static library");
        return;
    };
    let out = manifest.join("target/c_abi");
    std::fs::create_dir_all(&out).unwrap();
    let src = manifest.join("tests/c_abi/smoke.c");
    let inc = manifest.join("include");
    // System libraries that a Rust static library needs.
    let libs: &[&str] = if cfg!(target_os = "macos") {
        &["-lSystem", "-lc", "-lm"]
    } else {
        &["-lpthread", "-ldl", "-lm"]
    };
    for (name, compiler, lang) in [("smoke_c", &cc, "c"), ("smoke_cpp", &cxx, "c++")] {
        let exe = out.join(name);
        let std = if lang == "c" {
            "-std=c99"
        } else {
            "-std=c++17"
        };
        let status = Command::new(compiler)
            .args([std, "-Wall", "-Wextra", "-Werror", "-I"])
            .arg(&inc)
            .args(["-x", lang])
            .arg(&src)
            .args(["-x", "none"])
            .arg(&lib)
            .args(libs)
            .arg("-o")
            .arg(&exe)
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        let run = Command::new(&exe).output().unwrap();
        assert!(
            run.status.success(),
            "{name} failed: {}",
            String::from_utf8_lossy(&run.stderr)
        );
        assert!(String::from_utf8_lossy(&run.stdout).contains("c abi smoke ok"));
    }
}
