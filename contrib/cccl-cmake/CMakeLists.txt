# CUDA C++ Core Libraries: Thrust, CUB and libcu++. Header-only.
#
# The CUDA toolkit ships its own CCCL, but cuDF pins a specific version, so this one is
# placed BEFORE the toolkit's include path rather than relying on whichever the toolkit has.

if (NOT ENABLE_GPU)
    message (STATUS "Not using CCCL")
    return ()
endif ()

set (CCCL_DIR "${ClickHouse_SOURCE_DIR}/contrib/cccl")

add_library (_cccl INTERFACE)
target_include_directories (_cccl SYSTEM BEFORE INTERFACE
    "${CCCL_DIR}/libcudacxx/include"
    "${CCCL_DIR}/cub"
    "${CCCL_DIR}/thrust"
)
add_library (ch_contrib::cccl ALIAS _cccl)
