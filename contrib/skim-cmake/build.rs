fn main() {
    let mut build = cxx_build::bridge("src/lib.rs");
    build.file("src/check_abi.cpp");
    println!("cargo::rerun-if-changed=src/check_abi.cpp");
    build.compile("skim");
}
