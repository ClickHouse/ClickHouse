use std::error::Error;
use std::fs;

use blake3::Hasher;
use log::trace;

use crate::traits::compiler::Compiler;
use crate::traits::compiler::CompilerMeta;

pub struct ClangLike {
    compiler_path: String,

    args: Vec<String>,
    stripped_args: Vec<String>,

    input: String,
    output: String,

    relative_output: String,
}

impl ClangLike {
    pub fn from_args(compiler_path: String, args: Vec<String>) -> Self {
        let cwd = std::env::current_dir()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap()
            .trim_end_matches('/')
            .to_string();
        trace!("Current working directory: {}", cwd);

        let assumed_base_path = ClangLike::assume_base_path(&args);

        let stripped_args = args
            .iter()
            .map(|x| x.replace(&cwd, "/"))
            .map(|x| x.replace(&assumed_base_path, "/"))
            .collect::<Vec<String>>();

        ClangLike {
            compiler_path,

            input: ClangLike::get_input_from_args(&args),
            output: ClangLike::get_output_from_args(&args),

            relative_output: ClangLike::get_output_from_args(&stripped_args),

            args,
            stripped_args,
        }
    }

    fn assume_base_path(args: &Vec<String>) -> String {
        let cwd: String = match std::env::current_dir() {
            Ok(pathbuf) => pathbuf.into_os_string().into_string().unwrap(),
            _ => {
                panic!("Couldn't get current directory");
            }
        };

        let mut maybe_basepath: Vec<String> = vec![cwd];

        for (i, arg) in args.iter().enumerate() {
            if arg.starts_with("-I") {
                maybe_basepath.push(arg[2..].to_string());
                continue;
            }
            if arg == "-isystem" || arg == "-c" {
                maybe_basepath.push(args[i + 1].to_string());
                continue;
            }
            if arg.starts_with("--gcc-toolchain") {
                maybe_basepath.push(arg[16..].to_string());
                continue;
            }
            if arg.starts_with("--sysroot") {
                maybe_basepath.push(arg[10..].to_string());
                continue;
            }
        }

        let maybe_basepaths_sep_by_slash: Vec<Vec<String>> = maybe_basepath
            .into_iter()
            .map(|x| x.split("/").map(|x| x.to_string()).collect())
            .collect();
        let mut basepath = "".to_string();

        for i in 0..maybe_basepaths_sep_by_slash[0].len() {
            for j in 1..maybe_basepaths_sep_by_slash.len() {
                if maybe_basepaths_sep_by_slash[0][i] != maybe_basepaths_sep_by_slash[j][i] {
                    return basepath.trim_end_matches('/').to_string();
                }
            }
            basepath.push_str(&maybe_basepaths_sep_by_slash[0][i]);
            basepath.push_str("/");
        }

        basepath.trim_end_matches('/').to_string()
    }

    pub fn compiler_version(compiler_path: String) -> String {
        trace!("Using compiler: {}", compiler_path);

        let compiler_version = std::process::Command::new(compiler_path.clone())
            .arg("-dM")
            .arg("-E")
            .arg("-x")
            .arg("c")
            .arg("/dev/null")
            .output()
            .expect("Failed to execute command");

        let compiler_version = String::from_utf8_lossy(&compiler_version.stdout);

        let compiler_version = compiler_version
            .lines()
            .find(|x| x.starts_with("#define __VERSION__"))
            .unwrap()
            .split_whitespace()
            .skip(2)
            .collect::<Vec<&str>>()
            .join(" ");

        compiler_version.trim_matches('"').to_string()
    }

    fn get_output_from_args(args: &Vec<String>) -> String {
        let mut target = String::new();
        for (i, arg) in args.iter().enumerate() {
            if arg == "-o" {
                target = args[i + 1].to_string();
                break;
            }
        }
        target
    }

    fn get_input_from_args(args: &Vec<String>) -> String {
        let mut target = String::new();
        for (i, arg) in args.iter().enumerate() {
            if arg == "-c" {
                target = args[i + 1].to_string();
                break;
            }
        }
        target
    }

    fn hash_compiler_target(&self) -> String {
        let mut hasher = Hasher::new();

        let data = fs::read(&self.input).expect("Unable to read file");
        hasher.update(&data);
        hasher.update(&self.relative_output.as_bytes());

        hasher.finalize().to_hex().to_string()
    }

    fn hash_preprocessed_compiler_output(&self) -> String {
        let mut preprocess_args = vec![
            "-E".to_string(),
            "-P".to_string(),
            "-fminimize-whitespace".to_string(),
        ];
        preprocess_args.extend(self.args.clone());

        let output_flag_index = preprocess_args.iter().position(|x| x == "-o").unwrap();
        preprocess_args.remove(output_flag_index);
        preprocess_args.remove(output_flag_index);

        let output = std::process::Command::new(self.compiler_path.clone())
            .args(preprocess_args)
            .output()
            .expect("Failed to execute command");

        let stdout_output = String::from_utf8_lossy(&output.stdout);
        let stderr_output = String::from_utf8_lossy(&output.stderr);

        if stdout_output.is_empty() {
            panic!("{}", stderr_output);
        }

        let mut hasher = Hasher::new();
        hasher.update(stdout_output.as_bytes());

        hasher.finalize().to_hex().to_string()
    }
}

impl CompilerMeta for Clang {
    const NAME: &'static str = "clang";

    fn from_args(compiler_path: String, args: Vec<String>) -> Box<dyn Compiler> {
        assert!(compiler_path.ends_with(Clang::NAME));

        Box::new(Clang(ClangLike::from_args(compiler_path, args)))
    }
}

impl CompilerMeta for ClangXX {
    const NAME: &'static str = "clang++";

    fn from_args(compiler_path: String, args: Vec<String>) -> Box<dyn Compiler> {
        assert!(compiler_path.ends_with(ClangXX::NAME));

        Box::new(ClangXX(ClangLike::from_args(compiler_path, args)))
    }
}

impl Compiler for ClangLike {
    fn compile(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let output = std::process::Command::new(self.compiler_path.clone())
            .args(&self.args)
            .output()
            .unwrap();

        if !output.status.success() {
            println!("{}", String::from_utf8_lossy(&output.stdout));
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            std::process::exit(output.status.code().unwrap_or(1));
        }

        Ok(fs::read(&self.output)?)
    }

    fn version(&self) -> String {
        ClangLike::compiler_version(self.compiler_path.clone())
    }

    fn cache_key(&self) -> String {
        let mut hasher = Hasher::new();

        self.stripped_args
            .iter()
            .map(|x| x.as_bytes())
            .for_each(|x| {
                hasher.update(x);
            });

        let args_hash = hasher.finalize().to_string();

        let compiler_version = self.version();

        trace!("Compiler version: {}", compiler_version);

        let compiler_target_hash = ClangLike::hash_compiler_target(&self);
        let preprocessed_output_hash = self.hash_preprocessed_compiler_output();

        trace!("Compiler target hash: {}", compiler_target_hash);
        trace!("Preprocessed output hash: {}", preprocessed_output_hash);
        trace!("Args hash: {}", args_hash);

        hasher.reset();

        hasher.update(compiler_version.as_bytes());
        hasher.update(compiler_target_hash.as_bytes());
        hasher.update(preprocessed_output_hash.as_bytes());
        hasher.update(args_hash.as_bytes());

        hasher.finalize().to_string()
    }

    fn apply_cache(&self, binary: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        fs::write(&self.relative_output, binary)?;

        Ok(())
    }

    fn cacheable(&self) -> bool {
        true
    }
}

pub struct Clang(ClangLike);
pub struct ClangXX(ClangLike);

impl Compiler for ClangXX {
    fn compile(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        self.0.compile()
    }

    fn cache_key(&self) -> String {
        self.0.cache_key()
    }

    fn apply_cache(&self, binary: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.0.apply_cache(binary)
    }

    fn cacheable(&self) -> bool {
        self.0.cacheable()
    }

    fn version(&self) -> String {
        self.0.version()
    }
}

impl Compiler for Clang {
    fn compile(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        self.0.compile()
    }

    fn cache_key(&self) -> String {
        self.0.cache_key()
    }

    fn apply_cache(&self, binary: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.0.apply_cache(binary)
    }

    fn cacheable(&self) -> bool {
        self.0.cacheable()
    }

    fn version(&self) -> String {
        self.0.version()
    }
}
