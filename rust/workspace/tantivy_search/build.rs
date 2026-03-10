fn main() {
    let mut build = cxx_build::bridge("src/lib.rs");
    // C++17 is required for CXX bridge on some systems
    build.flag_if_supported("-std=c++17");
    build.compile("tantivy_search");
}
