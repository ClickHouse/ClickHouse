fn main() {
    let build = cxx_build::bridge("src/lib.rs");
    build.compile("skim");
}
