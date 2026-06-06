add_library(_blqsort INTERFACE)
target_include_directories(_blqsort SYSTEM BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/blqsort)
add_library(ch_contrib::blqsort ALIAS _blqsort)
