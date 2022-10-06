add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
)

if (OS_DARWIN AND NOT USE_STATIC_LIBRARIES)
    target_link_libraries (daemon PUBLIC -Wl,-undefined,dynamic_lookup)
endif()

target_link_libraries (daemon PUBLIC loggers common PRIVATE clickhouse_common_io clickhouse_common_config ${EXECINFO_LIBRARIES})

if (TARGET ch_contrib::sentry)
    target_link_libraries (daemon PRIVATE ch_contrib::sentry)
endif ()
