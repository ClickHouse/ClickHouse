set(FMT_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/fmtlib")

set (MODULE_SRCS ${FMT_SOURCE_DIR}/src/fmt.cc)

add_library(_fmt)
set_target_properties(_fmt PROPERTIES LINKER_LANGUAGE CXX)
target_compile_features(_fmt PUBLIC cxx_std_23)
target_sources(_fmt PUBLIC FILE_SET fmt TYPE CXX_MODULES BASE_DIRS ${FMT_SOURCE_DIR} FILES ${MODULE_SRCS})

target_include_directories(_fmt SYSTEM PUBLIC ../fmtlib/include)
target_compile_definitions (_fmt PUBLIC FMT_MODULE=1)
add_library(ch_contrib::fmt ALIAS _fmt)