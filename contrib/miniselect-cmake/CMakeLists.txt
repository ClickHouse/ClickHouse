add_library(_miniselect INTERFACE)
target_include_directories(_miniselect BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/miniselect/include)
add_library(ch_contrib::miniselect ALIAS _miniselect)
