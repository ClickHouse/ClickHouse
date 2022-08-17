option(USE_YAML_CPP "Enable yaml-cpp" ${ENABLE_LIBRARIES})

if (NOT USE_YAML_CPP)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/yaml-cpp/README.md")
    message (ERROR "submodule contrib/yaml-cpp is missing. to fix try run: \n git submodule update --init --recursive")
endif()
