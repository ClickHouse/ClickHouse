set(FASTPFOR_LIBRARY_DIR "${CMAKE_SOURCE_DIR}/contrib/FastPFOR")

set (SRCS
    "${FASTPFOR_LIBRARY_DIR}/src/bitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/bitpackingaligned.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/bitpackingunaligned.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/codecfactory.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/horizontalbitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/simdbitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/simdunalignedbitpacking.cpp"
    "${FASTPFOR_LIBRARY_DIR}/src/streamvbyte.c"
    "${FASTPFOR_LIBRARY_DIR}/src/varintdecode.c"
)

add_library(_fastpfor ${SRCS})

target_include_directories(_fastpfor SYSTEM BEFORE PUBLIC "${FASTPFOR_LIBRARY_DIR}/headers/")

if (ARCH_AARCH64)
    target_link_libraries(_fastpfor PRIVATE _simde)
    set(SIMDE_LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/simde)
    target_include_directories(_fastpfor SYSTEM BEFORE PUBLIC ${SIMDE_LIBRARY_DIR})
endif()

add_library(ch_contrib::fastpfor ALIAS _fastpfor)
