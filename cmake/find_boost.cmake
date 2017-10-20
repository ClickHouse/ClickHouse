option (USE_INTERNAL_BOOST_LIBRARY "Set to FALSE to use system boost library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    set (Boost_USE_STATIC_LIBS ${USE_STATIC_LIBRARIES})
    set (BOOST_ROOT "/usr/local")
    find_package (Boost 1.60 COMPONENTS program_options system filesystem thread)
    # incomplete, no include search, who use it?
    if (NOT Boost_FOUND)
        #    # Try to find manually.
        #    set (BOOST_PATHS "")
        #    find_library (Boost_PROGRAM_OPTIONS_LIBRARY boost_program_options PATHS ${BOOST_PATHS})
        #    find_library (Boost_SYSTEM_LIBRARY boost_system PATHS ${BOOST_PATHS})
    #    find_library (Boost_FILESYSTEM_LIBRARY boost_filesystem PATHS ${BOOST_PATHS})
        # maybe found but incorrect version.
        set (Boost_INCLUDE_DIRS "")
        set (Boost_SYSTEM_LIBRARY "")
    endif ()

endif ()

if (NOT Boost_SYSTEM_LIBRARY)
    set (USE_INTERNAL_BOOST_LIBRARY 1)
    set (Boost_PROGRAM_OPTIONS_LIBRARY boost_program_options_internal)
    set (Boost_SYSTEM_LIBRARY boost_system_internal)
    set (Boost_FILESYSTEM_LIBRARY boost_filesystem_internal)
    set (Boost_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/libboost/boost_1_65_0/")
endif ()

message (STATUS "Using Boost: ${Boost_INCLUDE_DIRS} : ${Boost_PROGRAM_OPTIONS_LIBRARY},${Boost_SYSTEM_LIBRARY},${Boost_FILESYSTEM_LIBRARY}")
