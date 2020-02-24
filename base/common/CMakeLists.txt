configure_file (config_common.h.in config_common.h)

set (SRCS
    argsToConfig.cpp
    coverage.cpp
    DateLUT.cpp
    DateLUTImpl.cpp
    demangle.cpp
    getMemoryAmount.cpp
    getThreadId.cpp
    JSON.cpp
    LineReader.cpp
    mremap.cpp
    phdr_cache.cpp
    preciseExp10.c
    setTerminalEcho.cpp
    shift10.cpp
    sleep.cpp
    terminalColors.cpp
)

if (ENABLE_REPLXX)
    set (SRCS ${SRCS}
        ReplxxLineReader.cpp
        ReplxxLineReader.h
    )
endif ()

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-include ${ClickHouse_SOURCE_DIR}/base/common/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

add_library (common ${SRCS})

target_include_directories(common PUBLIC .. ${CMAKE_CURRENT_BINARY_DIR}/..)

if (USE_INTERNAL_MEMCPY)
    target_link_libraries (common PRIVATE memcpy)
endif ()

if(CCTZ_INCLUDE_DIR)
    target_include_directories(common BEFORE PRIVATE ${CCTZ_INCLUDE_DIR})
endif()

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    target_include_directories (common SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
endif ()

if(NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (common SYSTEM BEFORE PUBLIC ${Poco_Foundation_INCLUDE_DIR})
endif()

if(CCTZ_LIBRARY)
    target_link_libraries(common PRIVATE ${CCTZ_LIBRARY})
endif()

target_link_libraries(common PUBLIC replxx)

target_link_libraries (common
    PUBLIC
        ${Poco_Util_LIBRARY}
        ${Poco_Foundation_LIBRARY}
        ${CITYHASH_LIBRARIES}
        ${Boost_SYSTEM_LIBRARY}
)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
