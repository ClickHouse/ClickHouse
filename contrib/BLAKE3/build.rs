use cbindgen;
use std::env;
use std::path::Path;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target = env::var("TARGET").unwrap();
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    match target.as_str() {
        "aarch64-unknown-linux-gnu" => {
            println!("cargo:rustc-link-lib=dylib={}/../sysroot/linux-aarch64/aarch64-linux-gnu/libc/usr/lib64/libc.so", crate_dir);
            println!("cargo:rustc-link-search=native={}/../sysroot/linux-aarch64/aarch64-linux-gnu/libc/usr/lib64", crate_dir);
        },
        "x86_64-apple-darwin" => {
            // no glibc requirements in https://doc.rust-lang.org/stable/rustc/platform-support.html
        },
        "x86_64-unknown-freebsd" => {
            println!("cargo:rustc-link-lib=dylib={}/../sysroot/freebsd-x86_64/usr/lib/libc.so", crate_dir);
            println!("cargo:rustc-link-search=native={}/../sysroot/freebsd-x86_64/usr/lib", crate_dir);
        },
        "powerpc64le-unknown-linux-gnu" => {
            println!("cargo:rustc-link-lib=dylib={}/../sysroot/linux-powerpc64le/powerpc64le-linux-gnu/libc/usr/lib64/libc.so", crate_dir);
            println!("cargo:rustc-link-search=native={}/../sysroot/linux-powerpc64le/powerpc64le-linux-gnu/libc/usr/lib64", crate_dir);
        },
        _ => {
            // default is x86_64-linux-gnu
            println!("cargo:rustc-link-lib=dylib={}/../sysroot/linux-x86_64/x86_64-linux-gnu/libc/usr/lib64/libc.so", crate_dir);
            println!("cargo:rustc-link-search=native={}/../sysroot/linux-x86_64/x86_64-linux-gnu/libc/usr/lib64", crate_dir);
        },
    };

    if Path::new("/include/blake3.h").exists() {
        let package_name = env::var("CARGO_PKG_NAME").unwrap();
        let output_file = ("include/".to_owned() + &format!("{}.h", package_name)).to_string();

        match cbindgen::generate(&crate_dir) {
            Ok(header) => {
                header.write_to_file(&output_file);
            }
            Err(err) => {
                panic!("{}", err)
            }
        }
    }

    Ok(())
}
