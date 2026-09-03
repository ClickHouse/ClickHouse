set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/zmij")

add_library(_zmij "${LIBRARY_DIR}/zmij.cc")
target_include_directories(_zmij SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/")
target_compile_features(_zmij PUBLIC cxx_std_20)
add_library(ch_contrib::zmij ALIAS _zmij)
