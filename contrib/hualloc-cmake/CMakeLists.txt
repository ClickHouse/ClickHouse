if (SANITIZE OR NOT (
	((OS_LINUX OR OS_FREEBSD) AND (ARCH_AMD64 OR ARCH_ARM OR ARCH_PPC64LE OR ARCH_RISCV64)) OR
    (OS_DARWIN AND (CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG"))))

    if (ENABLE_HUALLOC)
        message (${RECONFIGURE_MESSAGE_LEVEL}
                 "hualloc is disabled implicitly: it doesn't work with sanitizers and can only be used with x86_64, aarch64, or ppc64le Linux or FreeBSD builds and RelWithDebInfo macOS builds.")
    endif ()
    set (ENABLE_HUALLOC OFF)
else ()
    option (ENABLE_HUALLOC "Enable hualloc allocator" ${ENABLE_LIBRARIES})
endif ()

if (ENABLE_HUALLOC)
    add_library(_hualloc "${ClickHouse_SOURCE_DIR}/contrib/hualloc/hu_alloc.cpp")
    add_library(ch_contrib::hualloc ALIAS _hualloc)
endif()
