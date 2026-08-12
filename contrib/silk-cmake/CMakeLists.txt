if(NOT OS_LINUX)
    message(STATUS "Not building silk because it requires io_uring (Linux-only)")
    return()
endif()

if(NOT (ARCH_AMD64 OR ARCH_AARCH64))
    message(STATUS "Not building silk because it supports only x86_64 and aarch64")
    return()
endif()

if(NOT TARGET ch_contrib::liburing)
    message(STATUS "Not building silk because liburing is disabled")
    return()
endif()

if(NOT TARGET ch_contrib::librseq)
    message(STATUS "Not building silk because librseq is disabled")
    return()
endif()

set(SILK_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/silk")

# silk-util
file(GLOB SILK_UTIL_SOURCES "${SILK_SOURCE_DIR}/src/util/*.cpp")
add_library(_silk_util STATIC ${SILK_UTIL_SOURCES})
target_include_directories(_silk_util SYSTEM
    PUBLIC "${SILK_SOURCE_DIR}/include"
    PRIVATE "${SILK_SOURCE_DIR}/src"
)
target_link_libraries(_silk_util
    PUBLIC boost::headers_only
    PRIVATE ch_contrib::librseq
)
add_library(ch_contrib::silk_util ALIAS _silk_util)

# silk-fibers
file(GLOB SILK_FIBERS_SOURCES "${SILK_SOURCE_DIR}/src/fibers/*.cpp")
add_library(_silk_fibers STATIC ${SILK_FIBERS_SOURCES})
target_include_directories(_silk_fibers SYSTEM
    PUBLIC "${SILK_SOURCE_DIR}/include"
    PRIVATE "${SILK_SOURCE_DIR}/src"
)
target_link_libraries(_silk_fibers
    PUBLIC _silk_util
    PRIVATE boost::context ch_contrib::liburing
)
add_library(ch_contrib::silk_fibers ALIAS _silk_fibers)
