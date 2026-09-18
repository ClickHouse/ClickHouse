# nvcomp, NVIDIA's GPU compression library. libcudf's Parquet and ORC codecs go through it and
# there is no upstream switch to leave it out: src/io/comp/nvcomp_adapter.{cpp,cu,hpp,cuh},
# src/io/comp/{compression,decompression,common}.cpp and src/runtime/context.cpp name it
# unconditionally.
#
# Unlike the rest of contrib, the sources are not here. nvcomp is a proprietary binary and its
# license only grants redistribution as incorporated into an application, so neither the
# archives nor the headers are checked in (.gitignore's `*.a` matched the archives anyway).
# The tree is expected under contrib/nvcomp, laid out exactly the way NVIDIA's archive is:
#
#   contrib/nvcomp/include/nvcomp/*.h{,pp}
#   contrib/nvcomp/lib/libnvcomp_static.a
#
# Point GPU_NVCOMP_ROOT elsewhere to use an existing installation instead. Version 5.3.0.16
# is what cuDF 26.08 pins in cpp/cmake/thirdparty/get_nvcomp.cmake.

if (NOT ENABLE_GPU)
    message (STATUS "Not using nvcomp")
    return ()
endif ()

set (GPU_NVCOMP_ROOT "${ClickHouse_SOURCE_DIR}/contrib/nvcomp"
    CACHE PATH "Root of an nvcomp install, laid out as include/ and lib/")

set (NVCOMP_INCLUDE_DIR "${GPU_NVCOMP_ROOT}/include")
set (NVCOMP_LIBRARY "${GPU_NVCOMP_ROOT}/lib/libnvcomp_static.a")

if (NOT EXISTS "${NVCOMP_INCLUDE_DIR}/nvcomp.h" OR NOT EXISTS "${NVCOMP_LIBRARY}")
    message (FATAL_ERROR
        "nvcomp is missing from ${GPU_NVCOMP_ROOT}: expected ${NVCOMP_INCLUDE_DIR}/nvcomp.h and "
        "${NVCOMP_LIBRARY}. It is not redistributed in this repository - see "
        "contrib/nvcomp-cmake/CMakeLists.txt. Unpack "
        "https://developer.download.nvidia.com/compute/nvcomp/redist/nvcomp/linux-x86_64/"
        "nvcomp-linux-x86_64-5.3.0.16_cuda13-archive.tar.xz there, or set GPU_NVCOMP_ROOT to an "
        "existing install.")
endif ()

# What nvcomp's own nvcomp-targets-static.cmake declares: the archive, its include directory and
# a link-only dependency on the CUDA runtime. libnvcomp_cpu_static.a is left out, since cuDF
# links only nvcomp::nvcomp_static.
add_library (_nvcomp STATIC IMPORTED GLOBAL)
set_target_properties (_nvcomp PROPERTIES
    IMPORTED_LOCATION "${NVCOMP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${NVCOMP_INCLUDE_DIR}"
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${NVCOMP_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "$<LINK_ONLY:ch_gpu::cudart>")

add_library (ch_contrib::nvcomp ALIAS _nvcomp)
