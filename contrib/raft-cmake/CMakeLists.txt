# RAFT, header-only: the primitives cuVS is built on. Nothing of it is compiled here (upstream's
# RAFT_COMPILE_LIBRARY is off in the cuVS build too).
#
# include/raft/core/logger_macros.hpp is checked in, the only header upstream generates that the sources include. It is
# contrib/rapids-logger/cmake/logger_macros.hpp.in with the substitutions of upstream's
# create_logger_macros(RAFT "raft::default_logger()" include/raft/core), as contrib/rmm-cmake does it for RMM.
# Regenerate it when the submodule moves.

if (NOT ENABLE_GPU)
    message (STATUS "Not using RAFT")
    return ()
endif ()

add_library (_raft INTERFACE)
target_include_directories (_raft SYSTEM INTERFACE
    "${ClickHouse_SOURCE_DIR}/contrib/raft/cpp/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_definitions (_raft INTERFACE
    RAFT_LOG_ACTIVE_LEVEL=RAPIDS_LOGGER_LOG_LEVEL_INFO
    RAFT_SYSTEM_LITTLE_ENDIAN=1)
target_link_libraries (_raft INTERFACE
    ch_contrib::rapids_logger
    ch_contrib::rmm
    ch_contrib::cccl)
add_library (ch_contrib::raft ALIAS _raft)
