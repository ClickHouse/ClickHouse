option (ENABLE_RDKAFKA "Enable kafka" ON)

if (ENABLE_RDKAFKA)

option (USE_INTERNAL_RDKAFKA_LIBRARY "Set to FALSE to use system librdkafka instead of the bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_RDKAFKA_LIBRARY)
    find_library (RDKAFKA_LIBRARY rdkafka)
    find_path (RDKAFKA_INCLUDE_DIR NAMES librdkafka/rdkafka.h PATHS ${RDKAFKA_INCLUDE_PATHS})
endif ()

if (RDKAFKA_LIBRARY AND RDKAFKA_INCLUDE_DIR)
    include_directories (${RDKAFKA_INCLUDE_DIR})
else ()
    set (USE_INTERNAL_RDKAFKA_LIBRARY 1)
    set (RDKAFKA_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/src")
    set (RDKAFKA_LIBRARY rdkafka)
endif ()

set (USE_RDKAFKA 1)

endif ()

message (STATUS "Using librdkafka=${USE_RDKAFKA}: ${RDKAFKA_INCLUDE_DIR} : ${RDKAFKA_LIBRARY}")
