option (ENABLE_FASTMEMCPY "Enable FastMemcpy library (only internal)" ${ENABLE_LIBRARIES})

if (NOT OS_LINUX OR ARCH_AARCH64)
    set (ENABLE_FASTMEMCPY OFF)
endif ()

if (ENABLE_FASTMEMCPY)
    set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/FastMemcpy)

    set (SRCS
        ${LIBRARY_DIR}/FastMemcpy.c

        memcpy_wrapper.c
    )

    add_library (FastMemcpy ${SRCS})
    target_include_directories (FastMemcpy PUBLIC ${LIBRARY_DIR})

    target_compile_definitions(FastMemcpy PUBLIC USE_FASTMEMCPY=1)

    message (STATUS "Using FastMemcpy")
else ()
    add_library (FastMemcpy INTERFACE)

    target_compile_definitions(FastMemcpy INTERFACE USE_FASTMEMCPY=0)

    message (STATUS "Not using FastMemcpy")
endif ()
