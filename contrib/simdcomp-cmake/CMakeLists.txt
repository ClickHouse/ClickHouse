option (ENABLE_SIMDCOMP "Enable simdcomp" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SIMDCOMP OR NOT OS_LINUX OR NOT ARCH_AMD64)
    message(STATUS "Not using simdcomp")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/simdcomp")

set (SRCS
    "${LIBRARY_DIR}/src/avx512bitpacking.c"
    "${LIBRARY_DIR}/src/avxbitpacking.c"
    "${LIBRARY_DIR}/src/simdbitpacking.c"
    "${LIBRARY_DIR}/src/simdcomputil.c"
    "${LIBRARY_DIR}/src/simdfor.c"
    "${LIBRARY_DIR}/src/simdintegratedbitpacking.c"
    "${LIBRARY_DIR}/src/simdpackedsearch.c"
    "${LIBRARY_DIR}/src/simdpackedselect.c"
)

add_library (_simdcomp ${SRCS})

target_include_directories (_simdcomp SYSTEM PUBLIC
    "${LIBRARY_DIR}/include"
    "${LIBRARY_DIR}/src"
)

add_library (ch_contrib::simdcomp ALIAS _simdcomp)
