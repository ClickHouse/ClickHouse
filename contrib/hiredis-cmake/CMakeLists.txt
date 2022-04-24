option (ENABLE_REDIS_STREAMS "Enable redis streams" ${ENABLE_LIBRARIES})

if (NOT ENABLE_REDIS_STREAMS)
    message(STATUS "Not using hiredis")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/hiredis")

set(SRCS
        "${LIBRARY_DIR}/alloc.c"
        "${LIBRARY_DIR}/async.c"
        "${LIBRARY_DIR}/dict.c"
        "${LIBRARY_DIR}/hiredis.c"
        "${LIBRARY_DIR}/net.c"
        "${LIBRARY_DIR}/read.c"
        "${LIBRARY_DIR}/sds.c"
        "${LIBRARY_DIR}/sockcompat.c"
        "${LIBRARY_DIR}/test.c"
        )

add_library(_hiredis ${SRCS})
add_library(ch_contrib::hiredis ALIAS _hiredis)

target_include_directories(_hiredis SYSTEM PUBLIC "${LIBRARY_DIR}/adapters")
target_include_directories(_hiredis SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")   # for "hiredis/hiredis.h"
target_include_directories(_hiredis SYSTEM PUBLIC "${LIBRARY_DIR}")