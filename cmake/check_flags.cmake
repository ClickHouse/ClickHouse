include (CheckCXXCompilerFlag)
include (CheckCCompilerFlag)

check_cxx_compiler_flag("-Wsuggest-destructor-override" HAS_SUGGEST_DESTRUCTOR_OVERRIDE)
check_cxx_compiler_flag("-Wshadow" HAS_SHADOW)
check_cxx_compiler_flag("-Wsuggest-override" HAS_SUGGEST_OVERRIDE)
