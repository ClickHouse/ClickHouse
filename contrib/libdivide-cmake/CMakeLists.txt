set(LIBDIVIDE_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libdivide")
add_library (_libdivide INTERFACE)
# for libdivide.h
target_include_directories (_libdivide SYSTEM BEFORE INTERFACE ${LIBDIVIDE_SOURCE_DIR})
# for libdivide-config.h
target_include_directories (_libdivide SYSTEM BEFORE INTERFACE .)
add_library (ch_contrib::libdivide ALIAS _libdivide)
