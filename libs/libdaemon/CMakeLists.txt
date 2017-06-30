include_directories (include)

add_library (daemon
    src/BaseDaemon.cpp
    src/GraphiteWriter.cpp
    src/OwnPatternFormatter.cpp

    include/daemon/BaseDaemon.h
    include/daemon/GraphiteWriter.h
    include/daemon/OwnPatternFormatter.h
)

if (USE_UNWIND)
    target_include_directories (daemon BEFORE PRIVATE ${UNWIND_INCLUDE_DIR})
    add_definitions(-DUSE_UNWIND=1)
    target_link_libraries (daemon ${UNWIND_LIBRARY})
endif ()

target_link_libraries (daemon dbms)
target_include_directories (daemon PRIVATE ${ClickHouse_SOURCE_DIR}/libs/libpocoext/include)
