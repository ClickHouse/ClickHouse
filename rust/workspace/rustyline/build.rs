fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    // src/Client/RustylineCallbacks.h is included by the cxx-bridge stub.
    // Point clang at ClickHouse's src/ root so it resolves.
    let ch_src = format!("{manifest_dir}/../../../src");

    let mut build = cxx_build::bridge("src/lib.rs");
    build.include(ch_src);
    build.file("src/check_abi.cpp");
    build.compile("ch_rustyline");
}
