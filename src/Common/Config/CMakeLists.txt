set (SRCS
    AbstractConfigurationComparison.cpp
    ConfigProcessor.cpp
    configReadClient.cpp
    ConfigReloader.cpp
)

add_library(clickhouse_common_config ${SRCS})

target_include_directories(clickhouse_common_config PUBLIC ${DBMS_INCLUDE_DIR})
target_link_libraries(clickhouse_common_config
    PUBLIC
        common
        Poco::XML
    PRIVATE
        clickhouse_common_zookeeper
        string_utils
)
