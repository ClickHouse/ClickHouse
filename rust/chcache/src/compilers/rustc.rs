use blake3::Hasher;
use log::trace;
use std::cell::Cell;
use std::error::Error;
use std::fs::{self};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;

use crate::{
    compilers::clang::ClangLike,
    traits::compiler::{Compiler, CompilerMeta},
};

pub struct RustC {
    compiler_path: PathBuf,

    args: Vec<String>,
    out_dir: String,

    elapsed_compile_time_ms: Cell<Duration>,
}

impl CompilerMeta for RustC {
    const NAME: &'static str = "rustc";

    fn from_args(compiler_path: &Path, args: Vec<String>) -> Box<dyn Compiler> {
        let out_dir = args
            .iter()
            .position(|x| x == "--out-dir")
            .map(|x| args[x + 1].clone())
            .unwrap_or(String::new());

        Box::new(RustC {
            compiler_path: compiler_path.to_path_buf(),
            args,
            out_dir,

            elapsed_compile_time_ms: Cell::new(Duration::ZERO),
        })
    }
}

impl Compiler for RustC {
    fn cache_key(&self) -> String {
        let mut maybe_basepath: Vec<String> = vec![];

        for (i, arg) in self.args.iter().enumerate() {
            if arg.starts_with("--out-dir") {
                maybe_basepath.push(self.args[i + 1].to_string());
                continue;
            }
            if arg == "-C" || arg == "-L" {
                let next = self.args[i + 1].to_string();

                if next.starts_with("path=") {
                    maybe_basepath.push(next[5..].to_string());
                    continue;
                }

                if next.starts_with("dependency=") {
                    maybe_basepath.push(next[11..].to_string());
                    continue;
                }

                if next.starts_with("native=") {
                    maybe_basepath.push(next[7..].to_string());
                    continue;
                }

                continue;
            }
        }

        trace!("Maybe basepath: {:?}", maybe_basepath);

        let maybe_basepaths_sep_by_slash: Vec<Vec<String>> = maybe_basepath
            .into_iter()
            .map(|x| x.split("/").map(|x| x.to_string()).collect())
            .collect();
        let mut basepath = "".to_string();

        'outer: for i in 0..maybe_basepaths_sep_by_slash[0].len() {
            for j in 1..maybe_basepaths_sep_by_slash.len() {
                if maybe_basepaths_sep_by_slash[0][i] != maybe_basepaths_sep_by_slash[j][i] {
                    basepath = basepath.trim_end_matches('/').to_string();

                    break 'outer;
                }
            }

            basepath.push_str(&maybe_basepaths_sep_by_slash[0][i]);
            basepath.push_str("/");
        }

        basepath = basepath.trim_end_matches('/').to_string();
        trace!("Basepath: {:?}", basepath);
        assert!(!basepath.is_empty());
        assert!(!basepath.ends_with('/'));

        let cargo_manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        assert!(!cargo_manifest_dir.is_empty());

        let cargo_manifest_dir = std::path::Path::new(&cargo_manifest_dir);

        let mut stripped_args = self
            .args
            .clone()
            .iter()
            .map(|x| {
                if x.ends_with(".rs") {
                    let path = std::path::Path::new(x);
                    let stripped = {
                        let mut p_iter = cargo_manifest_dir.components();
                        let mut x_iter = path.components();

                        while let (Some(p), Some(xp)) = (p_iter.next(), x_iter.next()) {
                            if p != xp {
                                break;
                            }
                        }

                        x_iter.as_path()
                    };
                    trace!("Stripped path: {:?}", stripped);
                    stripped.to_string_lossy().into_owned()
                } else {
                    x.replace(&basepath, "./")
                }
            })
            .collect::<Vec<String>>();

        if let Some(index) = stripped_args.iter().position(|x| x == "--out-dir") {
            stripped_args.remove(index);
            stripped_args.remove(index);
        }

        if let Some(index) = stripped_args
            .iter()
            .position(|x| x.starts_with("--diagnostic-width"))
        {
            stripped_args.remove(index);
        }

        // May have different linkers between developers.
        // - "-C", "linker=/src/llvm/llvm-project/.cmake/bin/clang",
        // + "-C", "linker=/usr/bin/clang",
        let mut clang_linker_version: Option<String> = None;
        if let Some(index) = stripped_args.iter().position(|x| x.contains("linker=")) {
            let linker = stripped_args[index].replace("linker=", "");
            let linker = PathBuf::from(&linker);

            let linker_binary_name = linker
                .file_name()
                .and_then(|x| x.to_str())
                .unwrap_or("")
                .to_string();

            assert!(linker.is_file());
            assert!(
                linker_binary_name.starts_with("clang")
                    || linker_binary_name.starts_with("clang++")
            );

            clang_linker_version = Some(ClangLike::compiler_version(linker.as_path()));
            stripped_args.remove(index);
        }

        trace!("Stripped args: {:?}", stripped_args);

        let mut hasher = Hasher::new();

        stripped_args.iter().map(|x| x.as_bytes()).for_each(|x| {
            hasher.update(&x);
        });
        hasher.update(std::env::var("CARGO_PKG_NAME").unwrap().as_bytes());
        if clang_linker_version.is_some() {
            hasher.update(clang_linker_version.unwrap().as_bytes());
        }

        hasher.finalize().to_string()
    }

    fn version(&self) -> String {
        trace!("Using compiler: {}", self.compiler_path.display());

        let compiler_version = std::process::Command::new(self.compiler_path.clone())
            .arg("-V")
            .output()
            .expect("Failed to execute command");

        String::from_utf8_lossy(&compiler_version.stdout).to_string()
    }

    fn cacheable(&self) -> bool {
        if self.out_dir.is_empty() {
            return false;
        }

        if self.args.iter().any(|arg| {
            arg == "--version"
                || arg == "--help"
                || arg == "--explain"
                || arg == "-vV"
                || arg == "--print"
        }) {
            return false;
        }

        let has_input = self.args.iter().any(|arg| arg.ends_with(".rs"));
        if !has_input {
            return false;
        }

        if self.args.iter().any(|arg| arg.contains("emit=link")) {
            return false;
        }

        true
    }

    fn apply_cache(&self, bytes: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        trace!("Out dir: {:?}", self.out_dir);

        let cursor = Cursor::new(bytes);
        let mut archive = tar::Archive::new(cursor);
        archive.set_preserve_mtime(false);
        archive.unpack(&self.out_dir)?;

        Ok(())
    }

    fn compile(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let start_time = Instant::now();

        let output = std::process::Command::new(self.compiler_path.clone())
            .args(&self.args)
            .output()
            .unwrap();

        self.elapsed_compile_time_ms.set(start_time.elapsed());

        if !output.status.success() {
            println!("{}", String::from_utf8_lossy(&output.stdout));
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            std::process::exit(output.status.code().unwrap_or(1));
        }

        let files_to_pack = String::from_utf8_lossy(&output.stderr);
        let files_to_pack = files_to_pack
            .lines()
            .filter(|line| line.starts_with("{\"$message_type\":\"artifact\""))
            .map(|x| {
                let json: serde_json::Value = serde_json::from_str(x).unwrap();
                let artifact = json["artifact"].as_str().unwrap();
                let artifact = artifact.replace("\"", "");
                artifact
            })
            .collect::<Vec<String>>();

        trace!("Files to pack: {:?}", files_to_pack);

        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);
        let mut archive = tar::Builder::new(cursor);
        for file in &files_to_pack {
            let file = Path::new(&file);
            let filename = file.strip_prefix(&self.out_dir).unwrap();
            let filename = filename.to_str().unwrap();
            trace!("Packing file: {}", file.display());
            let mut packed_file = fs::File::open(file).unwrap();
            archive.append_file(filename, &mut packed_file).unwrap();
        }

        archive.finish().unwrap();
        drop(archive);

        Ok(buffer)
    }

    fn get_args(&self) -> Vec<String> {
        self.args.to_vec()
    }

    fn get_compile_duration(&self) -> u128 {
        self.elapsed_compile_time_ms.get().as_millis()
    }
}
