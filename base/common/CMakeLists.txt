set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/config_common.h)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/config_common.h.in ${CONFIG_COMMON})

add_library(apple_rt apple_rt.cpp)
target_include_directories (apple_rt PUBLIC ${COMMON_INCLUDE_DIR})
if (DEFINED APPLE_HAVE_CLOCK_GETTIME)
    target_compile_definitions(apple_rt PUBLIC -DAPPLE_HAVE_CLOCK_GETTIME=${APPLE_HAVE_CLOCK_GETTIME})
endif ()

set (COMMON_SRCS
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
)

if (ENABLE_REPLXX)
    set (COMMON_SRCS
        ReplxxLineReader.cpp
        ReplxxLineReader.h

        ${COMMON_SRCS}
    )
endif ()

add_library (common
    ${COMMON_SRCS}
    ${CONFIG_COMMON})

if (USE_INTERNAL_MEMCPY)
    set (MEMCPY_LIBRARIES memcpy)
endif ()

find_package (Threads)

if(CCTZ_INCLUDE_DIR)
    target_include_directories(common BEFORE PRIVATE ${CCTZ_INCLUDE_DIR})
endif()

target_include_directories (common PUBLIC ${COMMON_INCLUDE_DIR})

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    target_include_directories (common SYSTEM BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
endif ()

if(NOT USE_INTERNAL_POCO_LIBRARY)
    target_include_directories (common SYSTEM BEFORE PUBLIC ${Poco_Foundation_INCLUDE_DIR})
endif()

if(CCTZ_LIBRARY)
    target_link_libraries(common PRIVATE ${CCTZ_LIBRARY})
endif()

if (ENABLE_REPLXX)
    target_link_libraries(common PUBLIC replxx)
endif ()

target_link_libraries (common
        PUBLIC
    ${Poco_Util_LIBRARY}
    ${Poco_Foundation_LIBRARY}
    ${CITYHASH_LIBRARIES}
        PUBLIC
    ${Boost_SYSTEM_LIBRARY}
        PRIVATE
    ${MEMCPY_LIBRARIES})

if (RT_LIBRARY)
    target_link_libraries (common PRIVATE ${RT_LIBRARY})
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
