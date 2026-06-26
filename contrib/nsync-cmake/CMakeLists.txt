set(NSYNC_ENABLE_TESTS OFF CACHE BOOL "" FORCE)

set(_save_CMAKE_SKIP_INSTALL_RULES ${CMAKE_SKIP_INSTALL_RULES})
set(CMAKE_SKIP_INSTALL_RULES ON)

add_subdirectory("${ClickHouse_SOURCE_DIR}/contrib/nsync" "${CMAKE_CURRENT_BINARY_DIR}/nsync")

set(CMAKE_SKIP_INSTALL_RULES ${_save_CMAKE_SKIP_INSTALL_RULES})

target_include_directories(nsync_cpp SYSTEM PUBLIC "$<BUILD_INTERFACE:${ClickHouse_SOURCE_DIR}/contrib/nsync/public>")
target_compile_definitions(nsync_cpp PUBLIC NSYNC_ATOMIC_CPP11 NSYNC_USE_CPP11_TIMEPOINT)

add_library(ch_contrib::nsync ALIAS nsync_cpp)
