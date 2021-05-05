# The manticore is a header-only library of nlp functions.
set(MANTICORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/manticore/include")

add_library(manticore INTERFACE)

target_include_directories(manticore SYSTEM INTERFACE ${MANTICORE_INCLUDE_DIR})
