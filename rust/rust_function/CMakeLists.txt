corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_rust_function INTERFACE include)
add_library(ch_rust::rust_function ALIAS _ch_rust_rust_function)
