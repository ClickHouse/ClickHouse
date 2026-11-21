set(DARTS_CLONE_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/darts-clone")

add_library(_darts-clone INTERFACE)
target_include_directories(_darts-clone SYSTEM INTERFACE ${DARTS_CLONE_LIBRARY_DIR}/include)
add_library(ch_contrib::darts-clone ALIAS _darts-clone)
