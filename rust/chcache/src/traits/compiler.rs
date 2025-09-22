use std::{error::Error, path::Path};

pub trait CompilerMeta {
    const NAME: &'static str;

    fn from_args(compiler_path: &Path, args: Vec<String>) -> Box<dyn Compiler>;
}

pub trait Compiler {
    fn cacheable(&self) -> bool;
    fn cache_key(&self) -> String;
    fn version(&self) -> String;

    fn compile(&self) -> Result<Vec<u8>, Box<dyn Error>>;
    fn apply_cache(&self, source: &Vec<u8>) -> Result<(), Box<dyn Error>>;

    fn get_args(&self) -> Vec<String>;
    fn get_compile_duration(&self) -> u128;
}
