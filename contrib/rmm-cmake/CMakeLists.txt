# RAPIDS Memory Manager: the allocator cuDF works through.
#
# include/rmm/version_config.hpp is checked in rather than generated, following
# contrib/sz3-cmake. Regenerate it from rapids-cmake's cmake/template/version.h.in when the
# submodule moves - and keep the numbers unpadded, since `08` is not a valid C literal and
# the RMM_VERSION_GT/LT macros do arithmetic on them.

if (NOT ENABLE_GPU)
    message (STATUS "Not using RMM")
    return ()
endif ()

set (RMM_DIR "${ClickHouse_SOURCE_DIR}/contrib/rmm/cpp")

set (RMM_SOURCES
    "${RMM_DIR}/src/aligned.cpp"
    "${RMM_DIR}/src/cuda_device.cpp"
    "${RMM_DIR}/src/cuda_stream.cpp"
    "${RMM_DIR}/src/cuda_stream_pool.cpp"
    "${RMM_DIR}/src/cuda_stream_view.cpp"
    "${RMM_DIR}/src/device_buffer.cpp"
    "${RMM_DIR}/src/error.cpp"
    "${RMM_DIR}/src/exec_policy.cpp"
    "${RMM_DIR}/src/logger.cpp"
    "${RMM_DIR}/src/prefetch.cpp"
    "${RMM_DIR}/src/runtime_shutdown.cpp"
    "${RMM_DIR}/src/mr/aligned_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/arena_memory_resource.cpp"
    "${RMM_DIR}/src/mr/binning_memory_resource.cpp"
    "${RMM_DIR}/src/mr/callback_memory_resource.cpp"
    "${RMM_DIR}/src/mr/cuda_async_managed_memory_resource.cpp"
    "${RMM_DIR}/src/mr/cuda_async_memory_resource.cpp"
    "${RMM_DIR}/src/mr/cuda_async_view_memory_resource.cpp"
    "${RMM_DIR}/src/mr/cuda_memory_resource.cpp"
    "${RMM_DIR}/src/mr/fixed_size_memory_resource.cpp"
    "${RMM_DIR}/src/mr/limiting_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/logging_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/managed_memory_resource.cpp"
    "${RMM_DIR}/src/mr/pinned_host_memory_resource.cpp"
    "${RMM_DIR}/src/mr/pool_memory_resource.cpp"
    "${RMM_DIR}/src/mr/prefetch_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/sam_headroom_memory_resource.cpp"
    "${RMM_DIR}/src/mr/statistics_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/system_memory_resource.cpp"
    "${RMM_DIR}/src/mr/thread_safe_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/tracking_resource_adaptor.cpp"
    "${RMM_DIR}/src/mr/detail/aligned_resource_adaptor_impl.cpp"
    "${RMM_DIR}/src/mr/detail/arena_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/binning_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/callback_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/cuda_async_managed_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/cuda_async_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/fixed_size_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/limiting_resource_adaptor_impl.cpp"
    "${RMM_DIR}/src/mr/detail/logging_resource_adaptor_impl.cpp"
    "${RMM_DIR}/src/mr/detail/pool_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/prefetch_resource_adaptor_impl.cpp"
    "${RMM_DIR}/src/mr/detail/sam_headroom_memory_resource_impl.cpp"
    "${RMM_DIR}/src/mr/detail/statistics_resource_adaptor_impl.cpp"
    "${RMM_DIR}/src/mr/detail/thread_safe_resource_adaptor_impl.cpp"
    "${RMM_DIR}/src/mr/detail/tracking_resource_adaptor_impl.cpp"
)

add_library (_rmm ${RMM_SOURCES})

gpu_island_target (_rmm)

target_include_directories (_rmm SYSTEM PUBLIC
    "${RMM_DIR}/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_link_libraries (_rmm PUBLIC
    ch_contrib::cccl
    ch_contrib::rapids_logger
    ch_gpu::cudart
)

add_library (ch_contrib::rmm ALIAS _rmm)
