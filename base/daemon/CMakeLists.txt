add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
)

target_include_directories (daemon PUBLIC ..)

if (OS_DARWIN AND NOT USE_STATIC_LIBRARIES)
    target_link_libraries (daemon PUBLIC -Wl,-undefined,dynamic_lookup)
endif()

target_link_libraries (daemon PUBLIC loggers PRIVATE clickhouse_common_io clickhouse_common_config common ${EXECINFO_LIBRARIES})

if (TARGET ch_contrib::sentry)
    target_link_libraries (daemon PRIVATE ch_contrib::sentry)
endif ()
