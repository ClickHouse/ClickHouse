set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/magic_enum")
add_library (magic_enum INTERFACE)
target_include_directories(magic_enum SYSTEM INTERFACE ${LIBRARY_DIR}/include)
add_library(ch_contrib::magic_enum ALIAS magic_enum)
