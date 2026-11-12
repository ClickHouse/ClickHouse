//! Compiles `tests/c_abi/smoke.c` as C and as C++ against `include/avro_rust.h`,
//! links it with the static library this package builds, and runs it. Skipped
//! (loudly) when no C compiler is available, unless AVRO_REQUIRE_FIXTURES is set.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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

/// Use cargo's reported artifact filename: the build directory that the test runs in may differ
/// from the selected target directory (CMake below `build/.../out/`, cargo elsewhere).
fn static_lib() -> Option<PathBuf> {
    let mut cmd = Command::new(std::env::var_os("CARGO")?);
    cmd.args(["build", "--lib", "--offline", "--message-format=json", "--manifest-path"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"));
    if !cfg!(debug_assertions) {
        cmd.arg("--release");
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::inherit());
    let mut child = cmd.spawn().ok()?;
    let stdout = BufReader::new(child.stdout.take()?);
    let mut library = None;
    for line in stdout.lines().map_while(Result::ok) {
        let Some(filenames) = line
            .split(r#""reason":"compiler-artifact""#)
            .nth(1)
            .and_then(|line| line.split(r#""filenames":["#).nth(1))
            .and_then(|line| line.split(']').next())
        else {
            continue;
        };
        library = filenames
            .split(',')
            .map(|name| PathBuf::from(name.trim_matches('"')))
            .find(|path| path.file_name().is_some_and(|name| name == "lib_ch_rust_avro.a"));
    }
    let status = child.wait().ok()?;
    assert!(status.success(), "building the static library failed");
    let library = library?;
    library.exists().then_some(library)
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
