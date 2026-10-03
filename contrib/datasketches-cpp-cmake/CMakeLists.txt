option (ENABLE_DATASKETCHES "Enable DataSketches" ${ENABLE_LIBRARIES})

if (NOT ENABLE_DATASKETCHES)
    message(STATUS "Not using DataSketches")
    return()
endif()

set(DATASKETCHES_LIBRARY theta)
add_library(_datasketches INTERFACE)
# `include_patched/req` ships a copy of `req/include` with one fix on top of upstream
# `apache/datasketches-cpp@76edd74`: in `req_sketch::deserialize`, an `optional<T>`
# is engaged with `emplace` before `&*tmp` is taken, so the dereference is well-defined
# under hardened libc++. The unpatched upstream pattern causes UB / assertions under
# the hardened mode used by the ClickHouse `libcxx` build. Listed BEFORE the submodule
# include so the patched headers win the angle-include search; quote-includes inside
# the patched `req_sketch.hpp` for siblings like `req_common.hpp` then fall back to
# the unmodified copies in the submodule via the same include path list.
target_include_directories(_datasketches SYSTEM BEFORE INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include_patched/req"
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/common/include"
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/count/include"
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/theta/include"
    "${ClickHouse_SOURCE_DIR}/contrib/datasketches-cpp/req/include")

add_library(ch_contrib::datasketches ALIAS _datasketches)
