# rapids-logger, the logging layer rmm and cuDF are built against.
#
# Upstream generates a logger_macros.hpp per consumer via create_logger_macros(). The two
# copies cuDF and rmm need are checked in under include/ instead, the way
# contrib/sz3-cmake keeps its generated version.hpp. Regenerate them from
# contrib/rapids-logger/cmake/logger_macros.hpp.in when the submodule moves.

if (NOT ENABLE_GPU)
    message (STATUS "Not using rapids-logger")
    return ()
endif ()

set (RAPIDS_LOGGER_DIR "${ClickHouse_SOURCE_DIR}/contrib/rapids-logger")

add_library (_rapids_logger "${RAPIDS_LOGGER_DIR}/src/logger.cpp")

gpu_island_target (_rapids_logger)

target_include_directories (_rapids_logger SYSTEM PUBLIC
    "${RAPIDS_LOGGER_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
# Private, the way upstream links it: rapids-logger exists to keep spdlog out of its public
# API - none of its headers include spdlog - and PRIVATE keeps spdlog's include directories
# and SPDLOG_* definitions out of every consumer of ch_contrib::rapids_logger. For a static
# library CMake still carries the link dependency downstream on its own.
target_link_libraries (_rapids_logger PRIVATE ch_contrib::spdlog)

add_library (ch_contrib::rapids_logger ALIAS _rapids_logger)
