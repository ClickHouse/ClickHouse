---
description: 'RustライブラリをClickHouseに統合するためのガイド'
sidebar_label: 'Rustライブラリ'
slug: /development/integrating_rust_libraries
title: 'Rustライブラリの統合'
doc_type: 'guide'
---

Rustライブラリの統合について、BLAKE3ハッシュ関数の統合を例に説明します。

統合の最初の手順は、ライブラリを /rust フォルダーに追加することです。これを行うには、空のRustプロジェクトを作成し、必要なライブラリを Cargo.toml に追加する必要があります。また、Cargo.toml に `crate-type = ["staticlib"]` を追加し、新しいライブラリが静的ライブラリとしてコンパイルされるよう設定する必要もあります。

次に、Corrosion ライブラリを使用して、このライブラリを CMake にリンクする必要があります。最初の手順は、/rust フォルダー内の CMakeLists.txt にライブラリフォルダーを追加することです。その後、ライブラリディレクトリに CMakeLists.txt ファイルを追加する必要があります。その中で、Corrosion の import 関数を呼び出す必要があります。BLAKE3 を import するには、次の行を使用しました。

```CMake
corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
```

したがって、Corrosion を使って適切な CMake ターゲットを作成し、その後、より使いやすい名前にリネームします。`_ch_rust_blake3` という名前は Cargo.toml に由来しており、そこではプロジェクト名 (`name = "_ch_rust_blake3"`) として使用されています。

Rust のデータ型は C/C++ のデータ型と互換性がないため、この空のライブラリプロジェクトを使って、C/C++ から受け取ったデータの変換、ライブラリメソッドの呼び出し、および出力データの逆変換を行うためのシムメソッドを作成します。たとえば、BLAKE3 では次のメソッドを実装しました。

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

このメソッドは、入力として C 互換の文字列、そのサイズ、そして出力文字列へのポインタを受け取ります。続いて、C 互換の入力を実際のライブラリメソッドで使われる型に変換し、それらのメソッドを呼び出します。その後は、ライブラリメソッドの出力を C 互換の型に変換し直す必要があります。ただしこのケースでは、ライブラリが `fill()` メソッドによるポインタへの直接書き込みをサポートしていたため、この変換は不要でした。ここでの主なポイントは、メソッド数をできるだけ少なくすることです。そうすれば、各メソッド呼び出しで必要になる変換を減らせるため、オーバーヘッドも小さく抑えられます。

なお、この種のメソッドでは `#[no_mangle]` 属性と `extern "C"` はいずれも必須です。これらがないと、正しく C/C++ 互換でコンパイルできません。さらに、これらはインテグレーションの次のステップでも必要になります。

シムメソッドのコードを書いたら、次はライブラリ用のヘッダーファイルを準備する必要があります。これは手動で行うこともできますし、cbindgen ライブラリを使って自動生成することもできます。cbindgen を使う場合は、build.rs のビルドスクリプトを作成し、cbindgen を build-dependency として追加する必要があります。

ヘッダーファイルを自動生成できるビルドスクリプトの例:

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

また、C互換の属性にはそれぞれ、attribute #[no&#95;mangle] と `extern "C"` を使用する必要があります。これがないと、ライブラリが正しくコンパイルされなかったり、cbindgen によるヘッダーの自動生成が実行されなかったりします。

これらすべての手順を終えたら、小規模なプロジェクトでライブラリをテストし、互換性やヘッダー生成に関する問題を洗い出せます。ヘッダー生成時に問題が発生した場合は、`cbindgen.toml` ファイルで設定を調整してみてください (テンプレートはこちら: [https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)) 。

BLAKE3 の統合時に発生した問題についても、触れておく価値があります。
MemorySanitizer は、Rust の一部の変数が初期化済みかどうかを判別できないため、偽陽性のレポートを出すことがあります。これは、一部の変数に対してより明示的に定義した method を実装することで解決しましたが、この method 実装は低速であり、MemorySanitizer の builds を修正するためにのみ使用されます。