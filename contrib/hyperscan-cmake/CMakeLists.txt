option (ENABLE_HYPERSCAN "Enable hyperscan library" ${ENABLE_LIBRARIES})

if (NOT HAVE_SSSE3)
    set (ENABLE_HYPERSCAN OFF)
endif ()

if (ENABLE_HYPERSCAN)
    option (USE_INTERNAL_HYPERSCAN_LIBRARY "Use internal hyperscan library" ${NOT_UNBUNDLED})

    if (USE_INTERNAL_HYPERSCAN_LIBRARY)
        set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/hyperscan)

        set (SRCS
            ${LIBRARY_DIR}/src/hs.cpp
        )

        add_library (hyperscan ${SRCS})

        target_compile_definitions (hyperscan PUBLIC USE_HYPERSCAN=1)
        target_compile_options (hyperscan PRIVATE -g0)
        target_include_directories (hyperscan PUBLIC ${LIBRARY_DIR}/hs)
    else ()
        find_library (LIBRARY_HYPERSCAN hs)
        find_path (INCLUDE_HYPERSCAN NAMES hs/hs.h hs.h)

        add_library (hyperscan UNKNOWN IMPORTED)
        set_target_properties (hyperscan PROPERTIES IMPORTED_LOCATION ${LIBRARY_HYPERSCAN})
        set_target_properties (hyperscan PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_HYPERSCAN})

        target_compile_definitions (hyperscan INTERFACE USE_HYPERSCAN=1)
    endif ()

    message (STATUS "Using hyperscan")
else ()
    add_library (hyperscan INTERFACE)
    target_compile_definitions (hyperscan INTERFACE USE_HYPERSCAN=0)

    message (STATUS "Not using hyperscan")
endif ()
