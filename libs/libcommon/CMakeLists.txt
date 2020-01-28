set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/include/common/config_common.h)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/include/common/config_common.h.in ${CONFIG_COMMON})

add_library(apple_rt
    src/apple_rt.cpp
    include/port/clock.h
)
target_include_directories (apple_rt PUBLIC ${COMMON_INCLUDE_DIR})
if (DEFINED APPLE_HAVE_CLOCK_GETTIME)
    target_compile_definitions(apple_rt PUBLIC -DAPPLE_HAVE_CLOCK_GETTIME=${APPLE_HAVE_CLOCK_GETTIME})
endif ()

set (COMMON_SRCS
    src/argsToConfig.cpp
    src/coverage.cpp
    src/DateLUT.cpp
    src/DateLUTImpl.cpp
    src/demangle.cpp
    src/getMemoryAmount.cpp
    src/getThreadNumber.cpp
    src/JSON.cpp
    src/LineReader.cpp
    src/mremap.cpp
    src/phdr_cache.cpp
    src/preciseExp10.c
    src/setTerminalEcho.cpp
    src/shift10.cpp
    src/sleep.cpp

    include/common/constexpr_helpers.h
    include/common/coverage.h
    include/common/DateLUT.h
    include/common/DateLUTImpl.h
    include/common/DayNum.h
    include/common/demangle.h
    include/common/ErrorHandlers.h
    include/common/find_symbols.h
    include/common/getMemoryAmount.h
    include/common/getThreadNumber.h
    include/common/JSON.h
    include/common/likely.h
    include/common/LineReader.h
    include/common/LocalDate.h
    include/common/LocalDateTime.h
    include/common/logger_useful.h
    include/common/mremap.h
    include/common/phdr_cache.h
    include/common/preciseExp10.h
    include/common/setTerminalEcho.h
    include/common/shift10.h
    include/common/SimpleCache.h
    include/common/SimpleCache.h
    include/common/sleep.h
    include/common/strong_typedef.h
    include/common/Types.h

    include/ext/bit_cast.h
    include/ext/chrono_io.h
    include/ext/collection_cast.h
    include/ext/enumerate.h
    include/ext/function_traits.h
    include/ext/identity.h
    include/ext/map.h
    include/ext/range.h
    include/ext/scope_guard.h
    include/ext/size.h
    include/ext/unlock_guard.h
)

if (ENABLE_REPLXX)
    set (COMMON_SRCS
        src/ReplxxLineReader.cpp
        include/common/ReplxxLineReader.h

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
    add_subdirectory (src/tests)
endif ()
