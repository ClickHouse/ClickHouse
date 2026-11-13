# Enable libcxx hardening, see https://libcxx.llvm.org/Hardening.html
if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_EXTENSIVE")
else ()
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST")
endif ()

set (SILK_SUPPORTED OFF)
if (OS_LINUX AND (ARCH_AARCH64 OR (ARCH_AMD64 AND X86_ARCH_LEVEL VERSION_GREATER_EQUAL 2)))
    set (SILK_SUPPORTED ON)
endif ()

option (ENABLE_SILK "Use the Silk fiber library" ${SILK_SUPPORTED})

if (ENABLE_SILK AND NOT SILK_SUPPORTED)
    message (FATAL_ERROR "ENABLE_SILK requires Linux on x86-64-v2 or AArch64")
endif ()

if (ENABLE_SILK)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_LIBCPP_HAS_THREAD_API_EXTERNAL=1 -isystem ${ClickHouse_SOURCE_DIR}/base/libcxx-external-threading")
endif ()

disable_dummy_launchers_if_needed()
add_subdirectory(contrib/libcxxabi-cmake)
add_subdirectory(contrib/libcxx-cmake)
enable_dummy_launchers_if_needed()

# Exception handling library is embedded into libcxxabi.

target_link_libraries(global-libs INTERFACE cxx cxxabi)
