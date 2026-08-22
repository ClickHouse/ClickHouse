set (SRCS
    AbstractConfigurationComparison.cpp
    ConfigProcessor.cpp
    getClientConfigPath.cpp
    getLocalConfigPath.cpp
    ConfigReloader.cpp
    YAMLParser.cpp
    ConfigHelper.cpp
    parseConnectionCredentials.cpp
)

add_library(clickhouse_common_config ${SRCS})
target_link_libraries(clickhouse_common_config
    PUBLIC
        clickhouse_common_zookeeper_base
        clickhouse_common_zookeeper
        common
        Poco::XML
)

if (TARGET ch_contrib::yaml_cpp)
    target_link_libraries(clickhouse_common_config PRIVATE ch_contrib::yaml_cpp)
endif()
