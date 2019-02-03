add_library (daemon ${LINK_MODE}
    src/BaseDaemon.cpp
    src/GraphiteWriter.cpp
    src/ExtendedLogChannel.cpp
    src/OwnPatternFormatter.cpp
    src/OwnFormattingChannel.cpp
    src/OwnSplitChannel.cpp

    include/daemon/BaseDaemon.h
    include/daemon/GraphiteWriter.h
    include/daemon/ExtendedLogChannel.h
    include/daemon/OwnPatternFormatter.h
    include/daemon/OwnFormattingChannel.h
    include/daemon/OwnSplitChannel.h
    include/daemon/Pipe.h
)

target_include_directories (daemon PUBLIC include)
target_include_directories (daemon PRIVATE ${ClickHouse_SOURCE_DIR}/libs/libpocoext/include)

target_link_libraries (daemon PRIVATE clickhouse_common_io clickhouse_common_config common ${Poco_Net_LIBRARY} ${Poco_Util_LIBRARY} ${EXECINFO_LIBRARY} ${ELF_LIBRARY})
