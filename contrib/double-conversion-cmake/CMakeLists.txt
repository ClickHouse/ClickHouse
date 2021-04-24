SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/double-conversion")

add_library(double-conversion
"${LIBRARY_DIR}/double-conversion/bignum.cc"
"${LIBRARY_DIR}/double-conversion/bignum-dtoa.cc"
"${LIBRARY_DIR}/double-conversion/cached-powers.cc"
"${LIBRARY_DIR}/double-conversion/diy-fp.cc"
"${LIBRARY_DIR}/double-conversion/double-conversion.cc"
"${LIBRARY_DIR}/double-conversion/fast-dtoa.cc"
"${LIBRARY_DIR}/double-conversion/fixed-dtoa.cc"
"${LIBRARY_DIR}/double-conversion/strtod.cc")

target_include_directories(double-conversion SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}")
