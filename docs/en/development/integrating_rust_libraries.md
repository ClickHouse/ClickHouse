---
description: 'Guide for integrating Rust libraries into ClickHouse'
sidebar_label: 'Rust Libraries'
slug: /development/integrating_rust_libraries
title: 'Integrating Rust Libraries'
---

# Rust Libraries

Rust library integration will be described based on BLAKE3 hash-function integration.

The first step of integration is to add the library to /rust folder. To do this, you need to create an empty Rust project and include the required library in Cargo.toml. It is also necessary to configure new library compilation as static by adding `crate-type = ["staticlib"]` ​​to Cargo.toml.

Next, you need to link the library to CMake using Corrosion library. The first step is to add the library folder in the CMakeLists.txt inside the /rust folder. After that, you should add the CMakeLists.txt file to the library directory. In it, you need to call the Corrosion import function. These lines were used to import BLAKE3:

```CMake
corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
```

Thus, we will create a correct CMake target using Corrosion, and then rename it with a more convenient name. Note that the name `_ch_rust_blake3` comes from Cargo.toml, where it is used as project name (`name = "_ch_rust_blake3"`).

Since Rust data types are not compatible with C/C++ data types, we will use our empty library project to create shim methods for conversion of data received from C/C++, calling library methods, and inverse conversion for output data. For example, this method was written for BLAKE3:

```rust
#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
```
```rust
#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
) -> *mut c_char {
    if begin.is_null() {
        let err_str = CString::new("input was a null pointer").unwrap();
        return err_str.into_raw();
    }
    let mut hasher = blake3::Hasher::new();
    let input_bytes = CStr::from_ptr(begin);
    let input_res = input_bytes.to_bytes();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, blake3::OUT_LEN));
    std::ptr::null_mut()
}
```

This method gets C-compatible string, its size and output string pointer as input. Then, it converts C-compatible inputs into types that are used by actual library methods and calls them. After that, it should convert library methods' outputs back into C-compatible type. In that particular case library supported direct writing into pointer by method fill(), so the conversion was not needed. The main advice here is to create less methods, so you will need to do less conversions on each method call and won't create much overhead.

It is worth noting that the `#[no_mangle]` attribute and `extern "C"` are mandatory for all such methods. Without them, it will not be possible to perform a correct C/C++-compatible compilation. Moreover, they are necessary for the next step of the integration.

After writing the code for the shim methods, we need to prepare the header file for the library. This can be done manually, or you can use the cbindgen library for auto-generation. In case of using cbindgen, you will need to write a build.rs build script and include cbindgen as a build-dependency.

An example of a build script that can auto-generate a header file:

```rust
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

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
```

Also, you should use attribute #[no_mangle] and `extern "C"` for every C-compatible attribute. Without it library can compile incorrectly and cbindgen won't launch header autogeneration.

After all these steps you can test your library in a small project to find all problems with compatibility or header generation. If any problems occur during header generation, you can try to configure it with cbindgen.toml file (you can find a template here: [https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)).

It is worth noting the problem that occurred when integrating BLAKE3:
MemorySanitizer can cause false-positive reports as it's unable to see if some variables in Rust are initialized or not. It was solved with writing a method with more explicit definition for some variables, although this implementation of method is slower and is used only to fix MemorySanitizer builds.
