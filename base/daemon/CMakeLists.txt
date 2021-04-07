add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
)

target_include_directories (daemon PUBLIC ..)

if (OS_DARWIN AND NOT MAKE_STATIC_LIBRARIES)
    target_link_libraries (daemon PUBLIC -Wl,-undefined,dynamic_lookup)
endif()

target_link_libraries (daemon PUBLIC loggers PRIVATE clickhouse_common_io clickhouse_common_config common ${EXECINFO_LIBRARIES})

if (USE_SENTRY)
    target_link_libraries (daemon PRIVATE ${SENTRY_LIBRARY})
endif ()
