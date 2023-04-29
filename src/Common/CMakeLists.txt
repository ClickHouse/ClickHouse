add_subdirectory(StringUtils)

# list (APPEND PUBLIC_LIBS
#         ch_contrib::wyhash
#         ch_contrib::cityhash
#         ch_contrib::farmhash
#         clickhouse_dictionaries
#         clickhouse_dictionaries_embedded
#         clickhouse_parsers
#         ch_contrib::consistent_hashing
#         dbms
#         ch_contrib::metrohash
#         ch_contrib::murmurhash
#         ch_contrib::hashidsxx
#         ch_contrib::morton_nd
# )

if (ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_MYSQL)
    add_subdirectory (mysqlxx)
endif ()
