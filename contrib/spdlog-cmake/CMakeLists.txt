# spdlog, pulled in only because rapids-logger links it. Compiled (not header-only) so its
# symbols stay in one place, and with std::format so it does not need a second fmt beside
# contrib/fmtlib.

if (NOT ENABLE_GPU)
    message (STATUS "Not using spdlog")
    return ()
endif ()

set (SPDLOG_DIR "${ClickHouse_SOURCE_DIR}/contrib/spdlog")

add_library (_spdlog
    "${SPDLOG_DIR}/src/spdlog.cpp"
    "${SPDLOG_DIR}/src/stdout_sinks.cpp"
    "${SPDLOG_DIR}/src/color_sinks.cpp"
    "${SPDLOG_DIR}/src/file_sinks.cpp"
    "${SPDLOG_DIR}/src/async.cpp"
    "${SPDLOG_DIR}/src/cfg.cpp"
)

# Part of the cuDF island: shares libstdc++ with the device code.
gpu_island_target (_spdlog)

target_include_directories (_spdlog SYSTEM PUBLIC "${SPDLOG_DIR}/include")
target_compile_definitions (_spdlog PUBLIC SPDLOG_COMPILED_LIB SPDLOG_USE_STD_FORMAT)

add_library (ch_contrib::spdlog ALIAS _spdlog)
