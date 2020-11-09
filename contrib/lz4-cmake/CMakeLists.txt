option (USE_INTERNAL_LZ4_LIBRARY "Use internal lz4 library" ${NOT_UNBUNDLED})

if (USE_INTERNAL_LZ4_LIBRARY)
    set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/lz4)

    set (SRCS
        ${LIBRARY_DIR}/lib/lz4.c
        ${LIBRARY_DIR}/lib/lz4hc.c
        ${LIBRARY_DIR}/lib/lz4frame.c
        ${LIBRARY_DIR}/lib/xxhash.c
    )

    add_library (lz4 ${SRCS})

    target_compile_definitions (lz4 PUBLIC LZ4_DISABLE_DEPRECATE_WARNINGS=1 USE_XXHASH=1)
    if (SANITIZE STREQUAL "undefined")
        target_compile_options (lz4 PRIVATE -fno-sanitize=undefined)
    endif ()
    target_include_directories(lz4 PUBLIC ${LIBRARY_DIR}/lib)
else ()
    find_library (LIBRARY_LZ4 lz4)
    find_path (INCLUDE_LZ4 lz4.h)

    add_library (lz4 UNKNOWN IMPORTED)
    set_property (TARGET lz4 PROPERTY IMPORTED_LOCATION ${LIBRARY_LZ4})
    set_property (TARGET lz4 PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_LZ4})
    set_property (TARGET lz4 APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS USE_XXHASH=0)
endif ()
