# nvcomp is a prebuilt binary rather than a submodule - see contrib/nvcomp/README.md.
# Only the static archives are imported; ClickHouse links statically and the shipped
# shared objects are not kept.

if (NOT ENABLE_GPU)
    message (STATUS "Not using nvcomp")
    return ()
endif ()

set (NVCOMP_DIR "${ClickHouse_SOURCE_DIR}/contrib/nvcomp")

# The archive was built by NVIDIA against libstdc++ and needs a CUDA 13 runtime, so it
# may only be linked from the cuDF island (see src/GPU/GPUKernels.h on why that island
# exists). Linking it into a libc++ target would compile and then misbehave.
add_library (_nvcomp INTERFACE)

target_include_directories (_nvcomp SYSTEM INTERFACE "${NVCOMP_DIR}/include")
target_link_libraries (_nvcomp INTERFACE
    "${NVCOMP_DIR}/lib/libnvcomp_static.a"
    "${NVCOMP_DIR}/lib/libnvcomp_cpu_static.a"
)

add_library (ch_contrib::nvcomp ALIAS _nvcomp)
