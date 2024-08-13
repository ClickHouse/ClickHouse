add_library(_jwt-cpp INTERFACE)
target_include_directories(_jwt-cpp SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/jwt-cpp/include/")
add_library(ch_contrib::jwt-cpp ALIAS _jwt-cpp)