set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/acme-lw")

set (SRCS
    "${LIBRARY_DIR}/lib/acme-lw.cpp"
    "${LIBRARY_DIR}/lib/acme-exception.cpp"
)

add_library(_acme-lw ${SRCS})

target_include_directories(_acme-lw SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/internal" "${LIBRARY_DIR}")
target_compile_definitions(_acme-lw PUBLIC)

add_library(ch_contrib::acme-lw ALIAS _acme-lw)
