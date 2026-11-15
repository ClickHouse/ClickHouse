SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/double-conversion")

add_library(_double-conversion
    "${LIBRARY_DIR}/double-conversion/bignum-dtoa.cc"
    "${LIBRARY_DIR}/double-conversion/bignum.cc"
    "${LIBRARY_DIR}/double-conversion/cached-powers.cc"
    "${LIBRARY_DIR}/double-conversion/double-to-string.cc"
    "${LIBRARY_DIR}/double-conversion/fast-dtoa.cc"
    "${LIBRARY_DIR}/double-conversion/fixed-dtoa.cc"
    "${LIBRARY_DIR}/double-conversion/string-to-double.cc"
    "${LIBRARY_DIR}/double-conversion/strtod.cc"
)

target_include_directories(_double-conversion SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::double_conversion ALIAS _double-conversion)
