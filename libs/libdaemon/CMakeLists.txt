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
)

if (USE_UNWIND)
    target_compile_definitions (daemon PRIVATE USE_UNWIND=1)
    target_include_directories (daemon BEFORE PRIVATE ${UNWIND_INCLUDE_DIR})
    target_link_libraries (daemon ${UNWIND_LIBRARY})
endif ()

target_include_directories (daemon PUBLIC include)
target_include_directories (daemon PRIVATE ${ClickHouse_SOURCE_DIR}/libs/libpocoext/include)

target_link_libraries (daemon clickhouse_common_io clickhouse_common_config ${EXECINFO_LIBRARY} ${ELF_LIBRARY})
