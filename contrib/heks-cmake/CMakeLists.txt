set(HEKS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/heks/include")
add_library(_heks INTERFACE)
target_include_directories(_heks SYSTEM BEFORE INTERFACE ${HEKS_INCLUDE_DIR})
add_library(ch_contrib::heks ALIAS _heks)
