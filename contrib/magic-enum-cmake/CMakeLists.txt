set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/magic_enum")
add_library(_magic_enum INTERFACE)
target_include_directories(_magic_enum SYSTEM INTERFACE ${LIBRARY_DIR}/include)
add_library(ch_contrib::magic_enum ALIAS _magic_enum)
