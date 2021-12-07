option(ENABLE_ONETBB "Enable oneTBB" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ONETBB)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/oneTBB/CMakeLists.txt")
    message (WARNING "submodule contrib/oneTBB is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal oneTBB library")
    set (USE_ONETBB 0)
    return()
endif ()

if (NOT OS_FREEBSD)
    set (USE_ONETBB 1)
    set (ONETBB_LIBRARY tbb)
    set (ONETBB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/oneTBB/include")

    message (STATUS "Using oneTBB=${USE_ONETBB}: ${ONETBB_INCLUDE_DIR} : ${ONETBB_LIBRARY}")
else()
    set (USE_ONETBB 0)
    message (STATUS "Using internal oneTBB library on FreeBSD and Darwin is not supported")
endif()
