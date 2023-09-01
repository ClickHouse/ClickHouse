include (CheckCXXCompilerFlag)
include (CheckCCompilerFlag)

check_cxx_compiler_flag("-Wreserved-identifier" HAS_RESERVED_IDENTIFIER)
check_cxx_compiler_flag("-Wsuggest-destructor-override" HAS_SUGGEST_DESTRUCTOR_OVERRIDE)
check_cxx_compiler_flag("-Wsuggest-override" HAS_SUGGEST_OVERRIDE)
check_cxx_compiler_flag("-Xclang -fuse-ctor-homing" HAS_USE_CTOR_HOMING)
