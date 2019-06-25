add_library (daemon
    src/BaseDaemon.cpp
    src/GraphiteWriter.cpp

    include/daemon/BaseDaemon.h
    include/daemon/GraphiteWriter.h
)

if (USE_UNWIND)
    target_compile_definitions (daemon PRIVATE USE_UNWIND=1)
    target_include_directories (daemon BEFORE PRIVATE ${UNWIND_INCLUDE_DIR})
    target_link_libraries (daemon PRIVATE ${UNWIND_LIBRARY})
endif ()

target_include_directories (daemon PUBLIC include)

target_link_libraries (daemon PUBLIC loggers PRIVATE clickhouse_common_io clickhouse_common_config common ${Poco_Net_LIBRARY} ${Poco_Util_LIBRARY} ${EXECINFO_LIBRARIES})
