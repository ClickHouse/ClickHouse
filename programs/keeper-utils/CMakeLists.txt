if (NOT TARGET ch_contrib::nuraft)
    message (STATUS "Not building keeper-utils because NuRaft is disabled")
    return ()
endif ()

set (CLICKHOUSE_KEEPER_UTILS_SOURCES KeeperUtils.cpp)

set (CLICKHOUSE_KEEPER_UTILS_LINK
    PRIVATE
        clickhouse_functions
        dbms
)

clickhouse_program_add(keeper-utils)
