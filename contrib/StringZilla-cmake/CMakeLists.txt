set(STRINGZILLA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/StringZilla")
add_library(_stringzilla INTERFACE)
add_library(ch_contrib::stringzilla ALIAS _stringzilla)
target_include_directories(_stringzilla SYSTEM INTERFACE "${STRINGZILLA_SOURCE_DIR}/include")