option(ENABLE_BZIP2 "Enable bzip2 compression support" ${ENABLE_LIBRARIES})

if (NOT ENABLE_BZIP2)
    message (STATUS "bzip2 compression disabled")
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/bzip2/bzlib.h")
    message (WARNING "submodule contrib/bzip2 is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal bzip2 library")
    set (USE_NLP 0)
    return()
endif ()

set (USE_BZIP2 1)
set (BZIP2_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/bzip2")
set (BZIP2_LIBRARY bzip2)

message (STATUS "Using bzip2=${USE_BZIP2}: ${BZIP2_INCLUDE_DIR} : ${BZIP2_LIBRARY}")
