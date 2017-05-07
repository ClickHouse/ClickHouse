include_directories (include)
include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libcctz/include)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_include.cmake)

if (APPLE)
    if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Darwin" AND NOT "${CMAKE_SYSTEM_VERSION}" VERSION_LESS "16.1.0")
        set (APPLE_SIERRA_OR_NEWER 1)
    else ()
        set (APPLE_SIERRA_OR_NEWER 0)
    endif ()
endif ()

set (CONFIG_COMMON ${CMAKE_CURRENT_BINARY_DIR}/include/common/config_common.h)
configure_file (${CMAKE_CURRENT_SOURCE_DIR}/include/common/config_common.h.in ${CONFIG_COMMON})

if (APPLE)
    add_library (apple_rt
        src/apple_rt.cpp
        include/common/apple_rt.h
    )
endif ()

add_library (common
    src/DateLUT.cpp
    src/DateLUTImpl.cpp
    src/exp10.cpp
    src/JSON.cpp

    include/common/ApplicationServerExt.h
    include/common/Types.h
    include/common/MetrikaTypes.h
    include/common/DateLUT.h
    include/common/DateLUTImpl.h
    include/common/LocalDate.h
    include/common/LocalDateTime.h
    include/common/ErrorHandlers.h
    include/common/exp10.h
    include/common/likely.h
    include/common/logger_useful.h
    include/common/MultiVersion.h
    include/common/singleton.h
    include/common/strong_typedef.h
    include/common/JSON.h

    include/ext/bit_cast.hpp
    include/ext/collection_cast.hpp
    include/ext/enumerate.hpp
    include/ext/function_traits.hpp
    include/ext/identity.hpp
    include/ext/map.hpp
    include/ext/range.hpp
    include/ext/scope_guard.hpp
    include/ext/size.hpp
    include/ext/unlock_guard.hpp

    ${CONFIG_COMMON}
)

# When testing for memory leaks, dont link tcmalloc or jemalloc.

if (USE_JEMALLOC)
        message (STATUS "Link jemalloc : ${JEMALLOC_LIBRARIES}")
    set (MALLOC_LIBRARIES ${JEMALLOC_LIBRARIES})
elseif (USE_TCMALLOC)
    if (DEBUG_LIBTCMALLOC)
        message (STATUS "Link libtcmalloc_minimal_debug for testing: ${GPERFTOOLS_TCMALLOC_MINIMAL_DEBUG}")
        set (MALLOC_LIBRARIES ${GPERFTOOLS_TCMALLOC_MINIMAL_DEBUG})
    else ()
        message (STATUS "Link libtcmalloc_minimal : ${GPERFTOOLS_TCMALLOC_MINIMAL}")
        set (MALLOC_LIBRARIES ${GPERFTOOLS_TCMALLOC_MINIMAL})
    endif ()
else ()
    message (STATUS "Disabling libtcmalloc for valgrind better analysis")
endif ()

find_package (Threads)

target_link_libraries (
    common
    pocoext
    cctz
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
    ${MALLOC_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    ${RT_LIBRARIES})

if (ENABLE_TESTS)
    add_subdirectory (src/tests)
endif ()
