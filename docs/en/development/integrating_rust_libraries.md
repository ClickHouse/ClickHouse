# Integrating Rust libraries

Rust library integration will be described based on BLAKE3 hash-function integration.

The first step is forking a library and making neccessary changes for Rust and C/C++ compatibility.

After forking library repository you need to change target settings in Cargo.toml file. Firstly, you need to switch build to static library. Secondly, you need to add cbindgen crate to the crate list. We will use it later to generate C-header automatically.

The next step is creating or editing the build.rs script for your library - and enable cbindgen to generate the header during library build. These lines were added to BLAKE3 build script for the same purpose:

```
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

As you can see, script sets the output directory and launches header generation.

The next step is to add CMake files into library directory, so it can build with other submodules. As you can see, BLAKE3 main directory contains two CMake files - CMakeLists.txt and build_rust_lib.cmake. The second one is a function, which calls cargo build and sets all needed paths for library build. You should copy it to your library and then you can adjust cargo flags and other settings for you library needs.

When finished with CMake configuration, you should move on to create a C/C++ compatible API for your library. Let us see BLAKE3's method blake3_apply_shim:

```
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
    let mut hasher = Hasher::new();
    let input_bytes = CStr::from_ptr(begin);
    let input_res = input_bytes.to_bytes();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, OUT_LEN));
    std::ptr::null_mut()
}
```

This method gets C-compatible string, its size and output string pointer as input. Then, it converts C-compatible inputs into types that are used by actual library methods and calls them. After that, it should convert library methods' outputs back into C-compatible type. In that particular case library supported direct writing into pointer by method fill(), so the convertion was not needed. The main advice here is to create less methods, so you will need to do less convertions on each method call and won't create much overhead.

Also, you should use attribute #[no_mangle] and extern "C" for every C-compatible attribute. Without it library can compile incorrectly and cbindgen won't launch header autogeneration.

After all these steps you can test your library in a small project to find all problems with compatibility or header generation. If any problems occur during header generation, you can try to configure it with cbindgen.toml file (you can find an example of it in BLAKE3 directory or a template here: [https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)). If everything works correctly, you can finally integrate its methods into ClickHouse.

In addition, some problems with integration are worth noting here:
1) Some architectures may require special cargo flags or build.rs configurations, so you may want to test cross-compilation for different platforms first.
2) MemorySanitizer can cause false-positive reports as it's unable to see if some variables in Rust are initialized or not. It was solved with writing a method with more explicit definition for some variables, although this implementation of method is slower and is used only to fix MemorySanitizer builds.
