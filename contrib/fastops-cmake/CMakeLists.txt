if(ARCH_AMD64 AND NOT OS_FREEBSD AND NOT OS_DARWIN)
    option(ENABLE_FASTOPS "Enable fast vectorized mathematical functions library by Mikhail Parakhin" ${ENABLE_LIBRARIES})
elseif(ENABLE_FASTOPS)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Fastops library is supported on x86_64 only, and not FreeBSD or Darwin")
endif()

if(NOT ENABLE_FASTOPS)
    message(STATUS "Not using fastops")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/fastops")

set(SRCS "")

if(ARCH_AMD64)
    set (SRCS ${SRCS} "${LIBRARY_DIR}/fastops/avx/ops_avx.cpp")
    set_source_files_properties("${LIBRARY_DIR}/fastops/avx/ops_avx.cpp" PROPERTIES COMPILE_FLAGS "-mavx -DNO_AVX2")

    set (SRCS ${SRCS} "${LIBRARY_DIR}/fastops/avx2/ops_avx2.cpp")
    set_source_files_properties("${LIBRARY_DIR}/fastops/avx2/ops_avx2.cpp" PROPERTIES COMPILE_FLAGS "-mavx2 -mfma")
endif()

set (SRCS ${SRCS} "${LIBRARY_DIR}/fastops/plain/ops_plain.cpp" "${LIBRARY_DIR}/fastops/core/avx_id.cpp" "${LIBRARY_DIR}/fastops/fastops.cpp")

add_library(_fastops ${SRCS})

target_include_directories(_fastops SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::fastops ALIAS _fastops)
