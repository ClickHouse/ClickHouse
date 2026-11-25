# nvcomp, NVIDIA's GPU compression library, which libcudf names unconditionally. Not vendored
# here: its license only grants redistribution as part of an application, so the tree is expected
# under contrib/nvcomp and the error below says how to get it. 5.3.0.16 is what cuDF 26.08 pins.

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

# What nvcomp's own nvcomp-targets-static.cmake declares.
add_library (_nvcomp STATIC IMPORTED GLOBAL)
set_target_properties (_nvcomp PROPERTIES
    IMPORTED_LOCATION "${NVCOMP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${NVCOMP_INCLUDE_DIR}"
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${NVCOMP_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "$<LINK_ONLY:ch_gpu::cudart>")

add_library (ch_contrib::nvcomp ALIAS _nvcomp)
