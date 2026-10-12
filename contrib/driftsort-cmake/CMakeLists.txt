add_library(_driftsort INTERFACE)
target_include_directories(_driftsort SYSTEM BEFORE INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/driftsort)
add_library(ch_contrib::driftsort ALIAS _driftsort)
