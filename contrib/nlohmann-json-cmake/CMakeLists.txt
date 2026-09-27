set(JSON_DIR ${ClickHouse_SOURCE_DIR}/contrib/nlohmann-json)
add_library(_nlohmann_json INTERFACE)
target_include_directories(_nlohmann_json SYSTEM INTERFACE ${JSON_DIR}/single_include)
add_library(ch_contrib::nlohmann_json ALIAS _nlohmann_json)
