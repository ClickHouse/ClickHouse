---
description: '将 Rust 库集成到 ClickHouse 的指南'
sidebar_label: 'Rust 库'
slug: /development/integrating_rust_libraries
title: '集成 Rust 库'
doc_type: 'guide'
---

下面将以集成 BLAKE3 哈希函数为例，介绍如何集成 Rust 库。

集成的第一步是将库添加到 /rust 文件夹中。为此，你需要创建一个空的 Rust 项目，并在 Cargo.toml 中引入所需的库。还需要在 Cargo.toml 中添加 `crate-type = ["staticlib"]`，将新库配置为静态编译。

接下来，你需要使用 Corrosion 库将该库链接到 CMake。首先，在 /rust 文件夹中的 CMakeLists.txt 里添加该库文件夹。然后，在该库目录中添加 CMakeLists.txt 文件。在这个文件中，你需要调用 Corrosion 的导入函数。以下代码行用于导入 BLAKE3：

```CMake
corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
```

因此，我们将使用 Corrosion 创建一个正确的 CMake target，然后将其重命名为一个更便于使用的名称。请注意，`_ch_rust_blake3` 这个名称来自 Cargo.toml，在其中被用作项目名 (`name = "_ch_rust_blake3"`) 。

由于 Rust 数据类型与 C/C++ 数据类型并不兼容，我们将利用这个空的库项目来编写 shim 方法，用于转换从 C/C++ 接收的数据、调用库方法，以及将输出数据反向转换回来。例如，我们为 BLAKE3 编写了这个方法：

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

此方法接收 C 兼容字符串、其大小以及输出字符串指针作为输入。然后，它会将 C 兼容的输入转换为实际库方法使用的类型，并调用这些方法。之后，还需要将库方法的输出再转换回 C 兼容类型。在这个特定场景中，库支持通过 `fill()` 方法直接写入指针，因此无需进行这种转换。这里的主要建议是尽量减少方法数量，这样每次调用方法时就能减少转换操作，也不会带来太多额外开销。

值得注意的是，`#[no_mangle]` 属性和 `extern "C"` 对所有这类方法都是必需的。没有它们，就无法进行正确的 C/C++ 兼容编译。此外，它们也是集成下一步所必需的。

编写完 shim methods 的代码后，我们需要为该库准备头文件。这可以手动完成，也可以使用 cbindgen 库自动生成。如果使用 cbindgen，则需要编写一个 build.rs 构建脚本，并将 cbindgen 作为构建依赖引入。

可自动生成头文件的构建脚本示例：

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

此外，对于每个与 C 兼容的项，你都应使用 #[no&#95;mangle] 和 `extern "C"`。否则，库可能会被错误地编译，而且 cbindgen 也无法自动生成头文件。

完成上述所有步骤后，你就可以在一个小型项目中测试你的库，以找出兼容性或头文件生成方面的各种问题。如果在头文件生成过程中出现问题，可以尝试通过 `cbindgen.toml` 文件进行配置 (模板可在此处找到：[https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)) 。

值得一提的是，集成 BLAKE3 时曾遇到过一个问题：
MemorySanitizer 可能会产生误报，因为它无法判断 Rust 中某些变量是否已经初始化。这个问题最终通过编写一个对某些变量定义更明确的方法得到解决，不过这种方法的实现速度较慢，仅用于修复 MemorySanitizer 构建中的问题。