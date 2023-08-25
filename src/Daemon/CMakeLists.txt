configure_file(GitHash.cpp.in GitHash.generated.cpp)

add_library (daemon
    BaseDaemon.cpp
    GraphiteWriter.cpp
    SentryWriter.cpp
    MinidumpWriter.cpp
    GitHash.generated.cpp
)

target_link_libraries (daemon PUBLIC loggers common PRIVATE clickhouse_parsers clickhouse_common_io clickhouse_common_config)

if (TARGET ch_contrib::sentry)
    target_link_libraries (daemon PRIVATE ch_contrib::sentry dbms)
endif ()

if (TARGET ch_contrib::breakpad)
    target_link_libraries(daemon PUBLIC ch_contrib::breakpad dbms)
endif()