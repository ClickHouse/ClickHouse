# CUDA C++ Core Libraries: Thrust, CUB and libcu++. Header-only.
#
# The CUDA toolkit ships its own CCCL, but cuDF pins a specific version, so this one is
# placed BEFORE the toolkit's include path rather than relying on whichever the toolkit has.
#
# The pin is 3.6.0 as cuDF 26.12 pins it (cb9bee55) plus the commits up to NVIDIA/cccl#11926,
# which fixes the `std::align_val_t` forward declaration that is ambiguous with libc++. Code compiled
# by clang against libc++ includes CCCL through src/GPU/CCCL.h - see there.

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
