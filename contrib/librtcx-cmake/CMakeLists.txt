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

# GPU_NVRTC_* and GPU_NVJITLINK_* are resolved in cmake/cuda.cmake, by explicit path.
# Searching for them here would trip ci/jobs/scripts/check_style/check_cpp.sh.

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
    ch_gpu::cudart
)

# Without these two, rtcx.cpp defaults both to 0 and reaches nvrtc and nvJitLink through
# dlopen("libnvrtc.so.<major>") and dlsym, leaving the archives above unreferenced and the
# shared libraries a run-time requirement. Upstream sets the same pair from
# RTCX_STATIC_LINK_{NVRTC,NVJITLINK}; we always link the static ones.
target_compile_definitions (_rtcx PRIVATE
    RTCX_STATIC_LINK_LIBNVRTC=1
    RTCX_STATIC_LINK_LIBNVJITLINK=1
)

add_library (ch_contrib::rtcx ALIAS _rtcx)
