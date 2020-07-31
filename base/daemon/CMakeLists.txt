add_library (daemon
    src/BaseDaemon.cpp
    src/GraphiteWriter.cpp

    include/daemon/BaseDaemon.h
    include/daemon/GraphiteWriter.h
)

target_include_directories (daemon PUBLIC include)

target_link_libraries (daemon PUBLIC loggers PRIVATE clickhouse_common_io clickhouse_common_config common ${Poco_Net_LIBRARY} ${Poco_Util_LIBRARY} ${EXECINFO_LIBRARIES})
