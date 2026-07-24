use std::error::Error;

pub trait CompilerMeta {
    const NAME: &'static str;
    fn from_args(compiler_path: String, args: Vec<String>) -> Box<dyn Compiler>;
}

pub trait Compiler {
    fn cacheable(&self) -> bool;
    fn cache_key(&self) -> String;
    fn version(&self) -> String;

    fn compile(&self) -> Result<Vec<u8>, Box<dyn Error>>;
    fn apply_cache(&self, source: &Vec<u8>) -> Result<(), Box<dyn Error>>;
}
