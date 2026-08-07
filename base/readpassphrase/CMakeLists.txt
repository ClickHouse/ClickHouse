if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.c
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.h

add_library(readpassphrase readpassphrase.c)

set_target_properties(readpassphrase PROPERTIES LINKER_LANGUAGE C)
target_compile_options(readpassphrase PRIVATE -Wno-unused-result -Wno-reserved-id-macro -Wno-disabled-macro-expansion)
target_include_directories(readpassphrase PUBLIC .)
