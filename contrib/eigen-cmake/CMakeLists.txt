set(EIGEN_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/eigen")

add_library(_eigen INTERFACE)

target_include_directories(_eigen SYSTEM INTERFACE "${EIGEN_SOURCE_DIR}")

add_library(ch_contrib::eigen ALIAS _eigen)
