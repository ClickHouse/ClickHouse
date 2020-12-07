set(DRAGONBOX_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/dragonbox/include)

add_library(dragonbox INTERFACE)

target_include_directories(dragonbox SYSTEM INTERFACE ${DRAGONBOX_INCLUDE_DIR})