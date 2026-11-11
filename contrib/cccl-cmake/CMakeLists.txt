# CUDA C++ Core Libraries: Thrust, CUB and libcu++. Header-only.
#
# The CUDA toolkit ships its own CCCL, but cuDF pins a specific version, so this one is
# placed BEFORE the toolkit's include path rather than relying on whichever the toolkit has.
#
# The pin is 3.6.0 as cuDF 26.12 pins it (cb9bee55) plus the commits up to NVIDIA/cccl#11926,
# which fixes the `std::align_val_t` forward declaration that is ambiguous with libc++.
#
# libcu++ also forward-declares `std::get` for `std::array`, `std::tuple` and `std::pair` without
# libc++'s `abi_tag`, and clang then rejects libc++'s own declarations as adding the attribute in a
# redeclaration. ClickHouse's libc++ is built with `_LIBCPP_REMOVE_TRANSITIVE_INCLUDES`, so nothing
# brings those headers in first. So code compiled by clang against libc++ includes `<array>`,
# `<tuple>` and `<utility>` before any CCCL header, which makes libcu++'s declarations the
# redeclarations instead. Contrib targets may not set INTERFACE_COMPILE_OPTIONS, hence not here.

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
