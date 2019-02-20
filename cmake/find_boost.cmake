option (USE_INTERNAL_BOOST_LIBRARY "Set to FALSE to use system boost library instead of bundled" ${NOT_UNBUNDLED})

# Test random file existing in all package variants
if (USE_INTERNAL_BOOST_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/boost/libs/system/src/error_code.cpp")
   message (WARNING "submodules in contrib/boost is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_BOOST_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    set (Boost_USE_STATIC_LIBS ${USE_STATIC_LIBRARIES})
    set (BOOST_ROOT "/usr/local")
    find_package (Boost 1.60 COMPONENTS program_options system filesystem thread regex)
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
    set (Boost_SYSTEM_LIBRARY boost_system_internal)
    set (Boost_PROGRAM_OPTIONS_LIBRARY boost_program_options_internal)
    set (Boost_FILESYSTEM_LIBRARY boost_filesystem_internal ${Boost_SYSTEM_LIBRARY})
    set (Boost_REGEX_LIBRARY boost_regex_internal)

    set (Boost_INCLUDE_DIRS)

    set (BOOST_ROOT "${ClickHouse_SOURCE_DIR}/contrib/boost")

    # For boost from github:
    file (GLOB Boost_INCLUDE_DIRS_ "${ClickHouse_SOURCE_DIR}/contrib/boost/libs/*/include")
    list (APPEND Boost_INCLUDE_DIRS ${Boost_INCLUDE_DIRS_})
    # numeric has additional level
    file (GLOB Boost_INCLUDE_DIRS_ "${ClickHouse_SOURCE_DIR}/contrib/boost/libs/numeric/*/include")
    list (APPEND Boost_INCLUDE_DIRS ${Boost_INCLUDE_DIRS_})

    # For packaged version:
    list (APPEND Boost_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/boost")

endif ()

message (STATUS "Using Boost: ${Boost_INCLUDE_DIRS} : ${Boost_PROGRAM_OPTIONS_LIBRARY},${Boost_SYSTEM_LIBRARY},${Boost_FILESYSTEM_LIBRARY},${Boost_REGEX_LIBRARY}")
