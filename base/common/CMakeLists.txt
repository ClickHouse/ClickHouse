set (SRCS
    argsToConfig.cpp
    coverage.cpp
    DateLUT.cpp
    DateLUTImpl.cpp
    demangle.cpp
    getFQDNOrHostName.cpp
    getMemoryAmount.cpp
    getThreadId.cpp
    JSON.cpp
    mremap.cpp
    phdr_cache.cpp
    preciseExp10.cpp
    setTerminalEcho.cpp
    shift10.cpp
    sleep.cpp
    terminalColors.cpp
)

if (USE_DEBUG_HELPERS)
    set (INCLUDE_DEBUG_HELPERS "-include ${ClickHouse_SOURCE_DIR}/base/common/iostream_debug_helpers.h")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${INCLUDE_DEBUG_HELPERS}")
endif ()

add_library (common ${SRCS})

if (WITH_COVERAGE)
    target_compile_definitions(common PUBLIC WITH_COVERAGE=1)
else ()
    target_compile_definitions(common PUBLIC WITH_COVERAGE=0)
endif ()

if (USE_INTERNAL_CCTZ)
    set_source_files_properties(DateLUTImpl.cpp PROPERTIES COMPILE_DEFINITIONS USE_INTERNAL_CCTZ)
endif()

target_include_directories(common PUBLIC .. ${CMAKE_CURRENT_BINARY_DIR}/..)

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    target_include_directories (common SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
endif ()

target_link_libraries (common
    PUBLIC
        ${CITYHASH_LIBRARIES}
        ${Boost_SYSTEM_LIBRARY}
        FastMemcpy
        Poco::Net
        Poco::Net::SSL
        Poco::Util
        Poco::Foundation
        fmt

    PRIVATE
        cctz
)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
