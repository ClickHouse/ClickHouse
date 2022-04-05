if(ARCH_AMD64 OR ARCH_ARM)
    option (ENABLE_BASE64 "Enable base64" ${ENABLE_LIBRARIES})
elseif(ENABLE_BASE64)
    message (${RECONFIGURE_MESSAGE_LEVEL} "base64 library is only supported on x86_64 and aarch64")
endif()

if (NOT ENABLE_BASE64)
    message(STATUS "Not using base64")
    return()
endif()

SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/base64")

add_library(_base64_scalar OBJECT "${LIBRARY_DIR}/turbob64c.c" "${LIBRARY_DIR}/turbob64d.c")
add_library(_base64_ssse3 OBJECT "${LIBRARY_DIR}/turbob64sse.c") # This file also contains code for ARM NEON

if (ARCH_AMD64)
    add_library(_base64_avx OBJECT "${LIBRARY_DIR}/turbob64sse.c") # This is not a mistake. One file is compiled twice.
    add_library(_base64_avx2 OBJECT "${LIBRARY_DIR}/turbob64avx2.c")
endif ()

target_compile_options(_base64_scalar PRIVATE -falign-loops)

if (ARCH_AMD64)
    target_compile_options(_base64_ssse3 PRIVATE -mno-avx -mno-avx2 -mssse3 -falign-loops)
    target_compile_options(_base64_avx PRIVATE -falign-loops -mavx)
    target_compile_options(_base64_avx2 PRIVATE -falign-loops -mavx2)
else ()
    target_compile_options(_base64_ssse3 PRIVATE -falign-loops)
endif ()

if (ARCH_AMD64)
    add_library(_base64
        $<TARGET_OBJECTS:_base64_scalar>
        $<TARGET_OBJECTS:_base64_ssse3>
        $<TARGET_OBJECTS:_base64_avx>
        $<TARGET_OBJECTS:_base64_avx2>)
else ()
    add_library(_base64
        $<TARGET_OBJECTS:_base64_scalar>
        $<TARGET_OBJECTS:_base64_ssse3>)
endif ()

target_include_directories(_base64 SYSTEM PUBLIC ${LIBRARY_DIR})

if (XCODE OR XCODE_VERSION)
    # https://gitlab.kitware.com/cmake/cmake/issues/17457
    #     Some native build systems may not like targets that have only object files, so consider adding at least one real source file
    # This applies to Xcode.
    if (NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/dummy.c")
        file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/dummy.c" "")
    endif ()
    target_sources(_base64 PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/dummy.c")
endif ()

add_library(ch_contrib::base64 ALIAS _base64)
