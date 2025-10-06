set(STRINGZILLA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/StringZilla")

SET(sources "${STRINGZILLA_SOURCE_DIR}/c/stringzilla.c")

SET(headers
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/compare.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/find.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/hash.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/intersect.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/memory.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/small_string.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/sort.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/stringzilla.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/stringzilla.hpp"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/types.h"
        "${STRINGZILLA_SOURCE_DIR}/include/stringzilla/types.hpp"
)

add_library(_stringzilla ${sources} ${headers})
add_library(ch_contrib::stringzilla ALIAS _stringzilla)
target_include_directories(_stringzilla SYSTEM PUBLIC "${STRINGZILLA_SOURCE_DIR}/include")


if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
    set(SZ_PLATFORM_X86 TRUE)
    message(STATUS "Platform: x86")
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|AARCH64|arm64|ARM64")
    set(SZ_PLATFORM_ARM TRUE)
    message(STATUS "Platform: ARM")
endif ()

if (ARCH_AMD64)
    target_compile_definitions(
            _stringzilla PRIVATE "SZ_USE_WESTMERE=1" "SZ_USE_HASWELL=1" "SZ_USE_SKYLAKE=1" "SZ_USE_ICE=1"
            "SZ_USE_NEON=0" "SZ_USE_SVE=0" "SZ_USE_SVE2=0"
    )
elseif (ARCH_AARCH64)
    target_compile_definitions(
            _stringzilla PRIVATE "SZ_USE_WESTMERE=0" "SZ_USE_HASWELL=0" "SZ_USE_SKYLAKE=0" "SZ_USE_ICE=0" "SZ_USE_NEON=1"
            "SZ_USE_SVE=1" "SZ_USE_SVE2=1"
    )
endif ()

target_compile_definitions(_stringzilla PRIVATE "SZ_DYNAMIC_DISPATCH=1")
target_compile_definitions(_stringzilla PRIVATE "SZ_AVOID_LIBC=0")
target_compile_definitions(_stringzilla PRIVATE "SZ_DEBUG=0")