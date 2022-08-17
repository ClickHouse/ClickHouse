set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/dragonbox")

add_library(dragonbox_to_chars "${LIBRARY_DIR}/source/dragonbox_to_chars.cpp")

target_include_directories(dragonbox_to_chars SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include/")
