use cbindgen;
use std::env;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if Path::new("/include/blake3.h").exists() {
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
    }

    Ok(())
}
