include_directories (include)
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_include.cmake)

add_library (daemon
    src/BaseDaemon.cpp
    src/GraphiteWriter.cpp
    src/OwnPatternFormatter.cpp

    include/daemon/BaseDaemon.h
    include/daemon/GraphiteWriter.h
    include/daemon/OwnPatternFormatter.h
)

target_link_libraries (daemon dbms)
