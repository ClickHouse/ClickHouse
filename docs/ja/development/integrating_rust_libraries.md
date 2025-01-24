---
slug: /ja/development/integrating_rust_libraries
---
# Rustライブラリの統合

Rustライブラリの統合は、BLAKE3ハッシュ関数の統合を基に説明します。

統合の最初のステップは、ライブラリを/rustフォルダに追加することです。これを行うには、空のRustプロジェクトを作成し、Cargo.tomlに必要なライブラリを含めます。また、新しいライブラリのコンパイルを静的に設定するために、Cargo.tomlに`crate-type = ["staticlib"]`を追加する必要があります。

次に、Corrosionライブラリを使用してCMakeにライブラリをリンクする必要があります。最初のステップは、/rustフォルダ内のCMakeLists.txtにライブラリフォルダを追加することです。その後、ライブラリディレクトリにCMakeLists.txtファイルを追加します。この中で、Corrosionのインポート関数を呼び出す必要があります。BLAKE3をインポートするために以下の行が使用されました：

```
corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
```

このようにして、Corrosionを使用して正しいCMakeターゲットを作成し、その後より便利な名前にリネームします。なお、名前の`_ch_rust_blake3`はCargo.tomlから来ており、プロジェクト名（`name = "_ch_rust_blake3"`）として使用されています。

Rustのデータ型はC/C++のデータ型と互換性がないため、データを変換し、ライブラリメソッドを呼び出し、出力データを逆変換するためのShimメソッドを空のライブラリプロジェクトで作成します。例えば、このメソッドはBLAKE3のために書かれました：

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
    let mut hasher = blake3::Hasher::new();
    let input_bytes = CStr::from_ptr(begin);
    let input_res = input_bytes.to_bytes();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, blake3::OUT_LEN));
    std::ptr::null_mut()
}
```

このメソッドはC互換の文字列、そのサイズ、出力文字列ポインタを入力として取得します。それから、C互換の入力を実際のライブラリメソッドで使用される型に変換し、それらを呼び出します。その後、ライブラリメソッドの出力をC互換の型に戻して変換しなければなりません。この特定のケースでは、ライブラリがfill()メソッドによるポインタへの直接書き込みをサポートしていたため、変換は必要ありませんでした。ここでの主なアドバイスは、メソッドを少なく作成することで、各メソッド呼び出しの変換を少なくし、オーバーヘッドをあまり作成しないようにすることです。

`#[no_mangle]`属性と`extern "C"`は、すべてのそのようなメソッドに必須です。これらがなければ、正しいC/C++互換のコンパイルを行うことはできません。さらに、それらは統合の次のステップに必要です。

Shimメソッドのコードを記述した後、ライブラリのヘッダーファイルを準備する必要があります。これは手動で行うことも、cbindgenライブラリを使用して自動生成することもできます。cbindgenを使用する場合、build.rsビルドスクリプトを書き、cbindgenをビルド依存として含める必要があります。

ヘッダーファイルを自動生成できるビルドスクリプトの例：

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

また、すべてのC互換属性には`#[no_mangle]`および`extern "C"`属性を使用する必要があります。これがないと、ライブラリは誤ってコンパイルされる可能性があり、cbindgenはヘッダーの自動生成を起動しません。

これらすべてのステップの後、小さなプロジェクトでライブラリをテストして、互換性やヘッダー生成の問題をすべて見つけることができます。ヘッダー生成中に問題が発生した場合は、cbindgen.tomlファイルで設定を試みることができます（テンプレートはこちら：[https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)）。

BLAKE3の統合時に発生した問題として、MemorySanitizerがある変数がRustで初期化されているかどうかを確認できないため、偽陽性のレポートを生成することがあることは注意すべきです。これは、変数のより明示的な定義を持つメソッドを記述することで解決されましたが、このメソッドの実装は遅く、MemorySanitizerビルドを修正するためにのみ使用されます。
