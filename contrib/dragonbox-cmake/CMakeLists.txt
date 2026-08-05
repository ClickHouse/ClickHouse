set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/dragonbox")

add_library(_dragonbox_to_chars "${LIBRARY_DIR}/source/dragonbox_to_chars.cpp")
target_include_directories(_dragonbox_to_chars SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include/")
add_library(ch_contrib::dragonbox_to_chars ALIAS _dragonbox_to_chars)
