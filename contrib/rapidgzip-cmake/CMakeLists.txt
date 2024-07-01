add_library(_rapidgzip INTERFACE)

target_include_directories(_rapidgzip INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/rapidgzip/src/rapidgzip)
target_include_directories(_rapidgzip INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/rapidgzip/src/core)
target_include_directories(_rapidgzip INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/rapidgzip/src/indexed_bzip2)
target_include_directories(_rapidgzip INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/rapidgzip/src)

add_library(ch_contrib::rapidgzip ALIAS _rapidgzip)
