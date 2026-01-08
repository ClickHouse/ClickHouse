set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/incbin")
add_library(_incbin INTERFACE)
target_include_directories(_incbin SYSTEM INTERFACE ${LIBRARY_DIR})
add_library(ch_contrib::incbin ALIAS _incbin)
