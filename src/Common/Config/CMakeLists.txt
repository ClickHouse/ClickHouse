set (SRCS
    AbstractConfigurationComparison.cpp
    ConfigProcessor.cpp
    configReadClient.cpp
    ConfigReloader.cpp
    YAMLParser.cpp
    ConfigHelper.cpp
)

add_library(clickhouse_common_config ${SRCS})
target_link_libraries(clickhouse_common_config
    PUBLIC
        clickhouse_common_zookeeper
        common
        Poco::XML
    PRIVATE
        string_utils
)

add_library(clickhouse_common_config_no_zookeeper_log ${SRCS})
target_link_libraries(clickhouse_common_config_no_zookeeper_log
    PUBLIC
        clickhouse_common_zookeeper_no_log
        common
        Poco::XML
    PRIVATE
        string_utils
)

if (USE_YAML_CPP)
    target_link_libraries(clickhouse_common_config PRIVATE yaml-cpp)
    target_link_libraries(clickhouse_common_config_no_zookeeper_log PRIVATE yaml-cpp)
endif()
