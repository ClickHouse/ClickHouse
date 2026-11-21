if(NOT ENABLE_SILK)
    message(STATUS "Not using silk")
    return()
endif()

if(NOT TARGET ch_contrib::liburing)
    message(FATAL_ERROR "ENABLE_SILK requires liburing, pass -DENABLE_SILK=0 to build without Silk")
endif()

if(NOT TARGET ch_contrib::librseq)
    message(FATAL_ERROR "ENABLE_SILK requires librseq, pass -DENABLE_SILK=0 to build without Silk")
endif()

set(SILK_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/silk")

# silk-util
# CONFIGURE_DEPENDS makes Ninja re-run the glob on each build, so a submodule bump
# that adds or removes sources reconfigures already-existing build directories.
file(GLOB SILK_UTIL_SOURCES CONFIGURE_DEPENDS "${SILK_SOURCE_DIR}/src/util/*.cpp")
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
file(GLOB SILK_FIBERS_SOURCES CONFIGURE_DEPENDS "${SILK_SOURCE_DIR}/src/fibers/*.cpp")
add_library(_silk_fibers STATIC ${SILK_FIBERS_SOURCES} "${ClickHouse_SOURCE_DIR}/base/libcxx-external-threading/external_threading.cpp")
target_include_directories(_silk_fibers SYSTEM
    PUBLIC "${SILK_SOURCE_DIR}/include"
    PRIVATE "${SILK_SOURCE_DIR}/src"
)
target_link_libraries(_silk_fibers
    PUBLIC _silk_util
    PRIVATE boost::context ch_contrib::liburing
)
add_library(ch_contrib::silk_fibers ALIAS _silk_fibers)

target_link_libraries(cxx PUBLIC $<LINK_ONLY:ch_contrib::silk_fibers>)
