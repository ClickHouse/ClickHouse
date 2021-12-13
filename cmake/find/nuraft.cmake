option(ENABLE_NURAFT "Enable NuRaft" ${ENABLE_LIBRARIES})

if (NOT ENABLE_NURAFT)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/NuRaft/CMakeLists.txt")
    message (WARNING "submodule contrib/NuRaft is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal NuRaft library")
    set (USE_NURAFT 0)
    return()
endif ()

if (NOT OS_FREEBSD)
    set (USE_NURAFT 1)
    set (NURAFT_LIBRARY nuraft)

    set (NURAFT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/NuRaft/include")

    message (STATUS "Using NuRaft=${USE_NURAFT}: ${NURAFT_INCLUDE_DIR} : ${NURAFT_LIBRARY}")
else()
    set (USE_NURAFT 0)
    message (STATUS "Using internal NuRaft library on FreeBSD and Darwin is not supported")
endif()
