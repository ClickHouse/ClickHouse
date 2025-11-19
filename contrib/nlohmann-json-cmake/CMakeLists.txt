set(JSON_DIR ${ClickHouse_SOURCE_DIR}/contrib/nlohmann-json)
add_library(_nlohmann_json INTERFACE)
set_property(
    TARGET _nlohmann_json
    APPEND
    PROPERTY INTERFACE_INCLUDE_DIRECTORIES
             ${JSON_DIR}/single_include)
add_library(ch_contrib::nlohmann_json ALIAS _nlohmann_json)
