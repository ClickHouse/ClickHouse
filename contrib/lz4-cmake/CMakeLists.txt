option (USE_INTERNAL_LZ4_LIBRARY "Use internal lz4 library" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    find_library (LIBRARY_LZ4 lz4)
    find_path (INCLUDE_LZ4 lz4.h)

    if (LIBRARY_LZ4 AND INCLUDE_LZ4)
        set(EXTERNAL_LZ4_LIBRARY_FOUND 1)
        add_library (lz4 INTERFACE)
        set_property (TARGET lz4 PROPERTY INTERFACE_LINK_LIBRARIES  ${LIBRARY_LZ4})
        set_property (TARGET lz4 PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_LZ4})
        set_property (TARGET lz4 APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS USE_XXHASH=0)
    else()
        set(EXTERNAL_LZ4_LIBRARY_FOUND 0)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system lz4")
    endif()
endif()

if (NOT EXTERNAL_LZ4_LIBRARY_FOUND)
    set (USE_INTERNAL_LZ4_LIBRARY 1)
    set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/lz4")

    set (SRCS
        "${LIBRARY_DIR}/lib/lz4.c"
        "${LIBRARY_DIR}/lib/lz4hc.c"
        "${LIBRARY_DIR}/lib/lz4frame.c"
        "${LIBRARY_DIR}/lib/xxhash.c"
    )

    add_library (lz4 ${SRCS})

    target_compile_definitions (lz4 PUBLIC LZ4_DISABLE_DEPRECATE_WARNINGS=1 USE_XXHASH=1)
    if (SANITIZE STREQUAL "undefined")
        target_compile_options (lz4 PRIVATE -fno-sanitize=undefined)
    endif ()
    target_include_directories(lz4 PUBLIC ${LIBRARY_DIR}/lib)
endif ()
