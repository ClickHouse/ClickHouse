add_library(_pdqsort INTERFACE)
target_include_directories(_pdqsort SYSTEM BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/pdqsort)
add_library(ch_contrib::pdqsort ALIAS _pdqsort)
