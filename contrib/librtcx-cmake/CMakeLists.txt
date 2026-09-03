# librtcx: cuDF's runtime-compilation layer. cuDF embeds zstd-compressed CUDA source
# fragments into the binary and rtcx compiles them with NVRTC and links them with
# nvJitLink at query time, so nvrtc and nvJitLink are runtime requirements, not just
# build-time ones.
#
# Pinned to a bare commit because librtcx publishes no releases yet - see .gitmodules.

if (NOT ENABLE_GPU)
    message (STATUS "Not using librtcx")
    return ()
endif ()

set (RTCX_DIR "${ClickHouse_SOURCE_DIR}/contrib/librtcx")

find_library (GPU_NVRTC_LIBRARY NAMES nvrtc_static nvrtc
    HINTS "${GPU_CUDA_ROOT}" PATH_SUFFIXES lib64 lib REQUIRED)
find_library (GPU_NVRTC_BUILTINS_LIBRARY NAMES nvrtc-builtins_static nvrtc-builtins
    HINTS "${GPU_CUDA_ROOT}" PATH_SUFFIXES lib64 lib REQUIRED)
find_library (GPU_NVJITLINK_LIBRARY NAMES nvJitLink_static nvJitLink
    HINTS "${GPU_CUDA_ROOT}" PATH_SUFFIXES lib64 lib REQUIRED)
find_library (GPU_NVPTXCOMPILER_LIBRARY NAMES nvptxcompiler_static
    HINTS "${GPU_CUDA_ROOT}" PATH_SUFFIXES lib64 lib REQUIRED)
mark_as_advanced (GPU_NVRTC_LIBRARY GPU_NVRTC_BUILTINS_LIBRARY
                  GPU_NVJITLINK_LIBRARY GPU_NVPTXCOMPILER_LIBRARY)

add_library (_rtcx
    "${RTCX_DIR}/src/algorithm_launcher.cpp"
    "${RTCX_DIR}/src/algorithm_planner.cpp"
    "${RTCX_DIR}/src/fragment_entry.cpp"
    "${RTCX_DIR}/src/hash.cpp"
    "${RTCX_DIR}/src/nvjitlink_checker.cpp"
    "${RTCX_DIR}/src/nvrtc_lto_fragment_compiler.cpp"
    "${RTCX_DIR}/src/rtcx.cpp"
)

gpu_island_target (_rtcx)

target_include_directories (_rtcx SYSTEM PUBLIC "${RTCX_DIR}/include")
target_include_directories (_rtcx SYSTEM PRIVATE "${GPU_CUDA_ROOT}/include")
# zstd decompresses the embedded CUDA fragments. Reused from ClickHouse's contrib rather
# than vendored again - it is C, so it crosses the island boundary safely.
#
# $<LINK_ONLY:> matters: ch_contrib::zstd is an ordinary ClickHouse target, so it links
# global-group and would hand this island target global-group's INTERFACE_COMPILE_OPTIONS,
# -Werror among them - which nvcc reads as taking a value and applies to the next argument.
# The header comes in through target_include_directories instead.
target_include_directories (_rtcx SYSTEM PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/zstd/lib")
target_link_libraries (_rtcx PUBLIC
    $<LINK_ONLY:ch_contrib::zstd>
    "${GPU_NVRTC_LIBRARY}"
    "${GPU_NVRTC_BUILTINS_LIBRARY}"
    "${GPU_NVJITLINK_LIBRARY}"
    "${GPU_NVPTXCOMPILER_LIBRARY}"
    "${GPU_CUDART_LIBRARY}"
    "${GPU_CULIBOS_LIBRARY}"
)

add_library (ch_contrib::rtcx ALIAS _rtcx)
