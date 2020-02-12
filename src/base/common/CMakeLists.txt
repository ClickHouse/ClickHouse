set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/config_common.h)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/config_common.h.in ${CONFIG_COMMON})

add_library(apple_rt
    apple_rt.cpp
    port/clock.h
)
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

    constexpr_helpers.h
    coverage.h
    DateLUT.h
    DateLUTImpl.h
    DayNum.h
    demangle.h
    ErrorHandlers.h
    find_symbols.h
    getMemoryAmount.h
    getThreadId.h
    JSON.h
    likely.h
    LineReader.h
    LocalDate.h
    LocalDateTime.h
    logger_useful.h
    mremap.h
    phdr_cache.h
    preciseExp10.h
    setTerminalEcho.h
    shift10.h
    SimpleCache.h
    SimpleCache.h
    sleep.h
    strong_typedef.h
    Types.h

    ext/bit_cast.h
    ext/chrono_io.h
    ext/collection_cast.h
    ext/enumerate.h
    ext/function_traits.h
    ext/identity.h
    ext/map.h
    ext/push_back.h
    ext/range.h
    ext/scope_guard.h
    ext/size.h
    ext/unlock_guard.h
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
