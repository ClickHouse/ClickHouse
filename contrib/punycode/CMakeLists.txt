set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/punycode")

set (SRCS
    "${LIBRARY_DIR}/punycode.c"
)

add_library (_punycode ${SRCS})
target_include_directories(_punycode PUBLIC "${LIBRARY_DIR}")

target_compile_definitions(_punycode PUBLIC PUNYCODE_UINT=char32_t)

# "PUNYCODE_UINT=char32_t" is a C++ data type so we need a C++ compiler
SET_SOURCE_FILES_PROPERTIES(${SRCS} PROPERTIES LANGUAGE CXX)

add_library (ch_contrib::punycode ALIAS _punycode)
