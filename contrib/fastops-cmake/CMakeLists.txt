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

set(SRCS
    "${LIBRARY_DIR}/fastops/avx2/ops_avx2.cpp"
    "${LIBRARY_DIR}/fastops/fastops.cpp"
)

if (X86_ARCH_LEVEL VERSION_LESS 3)
    # Below v3: need runtime dispatch with AVX-only and Plain fallbacks.
    list(APPEND SRCS
        "${LIBRARY_DIR}/fastops/avx/ops_avx.cpp"
        "${LIBRARY_DIR}/fastops/plain/ops_plain.cpp"
        "${LIBRARY_DIR}/fastops/core/avx_id.cpp"
    )
    set_source_files_properties("${LIBRARY_DIR}/fastops/avx/ops_avx.cpp" PROPERTIES COMPILE_FLAGS "-mavx -DNO_AVX2")
    set_source_files_properties("${LIBRARY_DIR}/fastops/avx2/ops_avx2.cpp" PROPERTIES COMPILE_FLAGS "-mavx2 -mfma -DNO_AVX")
endif()

add_library(_fastops ${SRCS})

target_include_directories(_fastops SYSTEM PUBLIC "${LIBRARY_DIR}")

add_library(ch_contrib::fastops ALIAS _fastops)
